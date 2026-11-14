#define NOMINMAX
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#pragma comment(lib, "ws2_32.lib")

const int STUDENT_ID = 82;
const int PROTOCOL_VERSION = 1;
const int MAX_ENTRIES = 1000;
const int MAX_KEY = 32;
const int MAX_VALUE = 256;
const int MAX_MSG = 65536;
const int POLL_MS = 100;                // how often waiting loops check the stop flag and the timeout

enum { R_OK = 1, R_CLOSED = 0, R_ERROR = -1, R_TIMEOUT = -2, R_STOP = -3, R_BADLEN = -4 };

static std::string HOST = "127.0.0.1";
static int PORT = 20000 + STUDENT_ID;
static int TIMEOUT_MS = 30000;          // max wait for the next request from a connected client
static std::string CSV_PATH;            // request log (empty: no CSV)
static bool VERBOSE = false;            // log every request on the console

static std::map<std::string, std::string> g_table;
static std::mutex g_tableMx;            // every table operation is atomic for other clients
static std::mutex g_printMx;
static std::atomic<int> g_stop(0), g_active(0), g_connections(0), g_requests(0), g_rejected(0);


// ---------- logging and errors ----------

static void Print(const std::string& s) {
    std::lock_guard<std::mutex> lock(g_printMx);
    std::cout << s << std::flush;
}


static double NowMs() {
    LARGE_INTEGER f, c;
    QueryPerformanceFrequency(&f);
    QueryPerformanceCounter(&c);
    return c.QuadPart * 1000.0 / f.QuadPart;
}


static std::string MakeTime() {
    SYSTEMTIME st;
    GetLocalTime(&st);
    char t[16];
    sprintf_s(t, "%02d:%02d:%02d.%03d", st.wHour, st.wMinute, st.wSecond, st.wMilliseconds);
    return t;
}


static void Log(const std::string& s) {
    Print("[" + MakeTime() + "] " + s + "\n");
}


// code: WSAGetLastError() for Winsock calls, GetLastError() for the rest
static void PrintError(const std::string& op, int code) {
    char msg[256] = "";
    FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL, code, MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US), msg, sizeof(msg), NULL);
    std::string text = msg;
    while (!text.empty() && isspace((unsigned char)text.back())) text.pop_back();
    Print("Error: " + op + ", code " + std::to_string(code) + ", " + text + "\n");
}


// ---------- framing ----------

// Sends one framed message; handles partial send().
static bool SendMsg(SOCKET s, const std::string& payload) {
    uint32_t net = htonl((u_long)payload.size());
    std::string buf((const char*)&net, 4);
    buf += payload;
    for (size_t sent = 0; sent < buf.size();) {
        int r = send(s, buf.data() + sent, (int)(buf.size() - sent), 0);
        if (r == SOCKET_ERROR) {
            PrintError("send", WSAGetLastError());
            return false;
        }
        sent += r;
    }
    return true;
}


// Reads exactly len bytes; handles partial recv(). Waits with select() in POLL_MS slices,
// so the stop flag is noticed and the idle timeout is enforced.
static int RecvAll(SOCKET s, char* buf, int len) {
    int got = 0, waited = 0;
    while (got < len) {
        if (g_stop) return R_STOP;
        fd_set rd;
        FD_ZERO(&rd);
        FD_SET(s, &rd);
        timeval tv = { 0, POLL_MS * 1000 };
        int r = select(0, &rd, NULL, NULL, &tv);
        if (r == SOCKET_ERROR) {
            PrintError("select", WSAGetLastError());
            return R_ERROR;
        }
        if (r == 0) {
            waited += POLL_MS;
            if (waited >= TIMEOUT_MS) return R_TIMEOUT;
            continue;
        }
        int n = recv(s, buf + got, len - got, 0);
        if (n == 0) return got == 0 ? R_CLOSED : R_ERROR;   // closed in the middle of a message
        if (n == SOCKET_ERROR) {
            PrintError("recv", WSAGetLastError());
            return R_ERROR;
        }
        got += n;
        waited = 0;
    }
    return R_OK;
}


// Reads one framed message. Length 0 or > MAX_MSG gives R_BADLEN.
static int RecvMsg(SOCKET s, std::string& payload) {
    uint32_t net;
    int rc = RecvAll(s, (char*)&net, 4);
    if (rc != R_OK) return rc;
    uint32_t len = ntohl(net);
    if (len == 0 || len > (uint32_t)MAX_MSG) return R_BADLEN;
    payload.assign(len, '\0');
    rc = RecvAll(s, &payload[0], (int)len);
    return rc == R_CLOSED ? R_ERROR : rc;
}


static const char* Describe(int rc) {
    switch (rc) {
    case R_CLOSED:  return "closed by client";
    case R_TIMEOUT: return "idle timeout";
    case R_STOP:    return "server stopping";
    case R_BADLEN:  return "invalid message length";
    default:        return "receive error";
    }
}


// ---------- request processing ----------

struct Reply {
    int reqId = 0, clientId = 0;
    std::string cmd, status, data, key;
    int entries = -1;                   // table size after the operation (-1: not reached the table)
};


// Parses and executes one request. All table access is under g_tableMx.
static Reply Execute(const std::string& payload) {
    Reply r;
    std::istringstream in(payload);
    int ver = 0;
    in >> ver >> r.reqId >> r.clientId >> r.cmd;

    bool printable = std::all_of(payload.begin(), payload.end(), [](char c) { return (unsigned char)c >= 0x20; });
    std::string key, value, extra;
    bool needKey = (r.cmd == "PUT" || r.cmd == "GET" || r.cmd == "DELETE");

    if (in.fail() || !printable) { r.status = "BAD_REQUEST"; return r; }
    if (ver != PROTOCOL_VERSION) { r.status = "BAD_VERSION"; return r; }
    if (!needKey && r.cmd != "LIST" && r.cmd != "COUNT") { r.status = "UNKNOWN_CMD"; return r; }

    if (needKey) in >> key;
    if (r.cmd == "PUT") std::getline(in >> std::ws, value);     // value is the rest of the line, may contain spaces
    else if (in >> extra) key.clear(), extra = "?";             // extra argument
    r.key = key;
    bool bad = (needKey && (key.empty() || (int)key.size() > MAX_KEY)) || !extra.empty() ||
        (r.cmd == "PUT" && (value.empty() || (int)value.size() > MAX_VALUE));
    if (bad) { r.status = "BAD_REQUEST"; return r; }

    std::lock_guard<std::mutex> lock(g_tableMx);
    auto it = g_table.find(key);
    r.status = "OK";
    if (r.cmd == "PUT") {
        if (it != g_table.end()) { it->second = value; r.data = "UPDATED"; }
        else if ((int)g_table.size() >= MAX_ENTRIES) r.status = "FULL";
        else { g_table[key] = value; r.data = "NEW"; }
    }
    else if (r.cmd == "GET" || r.cmd == "DELETE") {
        if (it == g_table.end()) r.status = "NOT_FOUND";
        else if (r.cmd == "GET") r.data = it->second;
        else g_table.erase(it);
    }
    else if (r.cmd == "COUNT") {
        r.data = std::to_string(g_table.size());
    }
    else {      // LIST: keys only, at most 1000 * 33 bytes, always below MAX_MSG
        for (auto& kv : g_table) r.data += (r.data.empty() ? "" : " ") + kv.first;
    }
    r.entries = (int)g_table.size();
    return r;
}


// ---------- request log (CSV) ----------

static FILE* g_csv = NULL;
static std::mutex g_csvMx;
static double g_startMs = 0;            // t_ms in the CSV counts from the server start


static bool OpenCsv() {
    if (fopen_s(&g_csv, CSV_PATH.c_str(), "a") != 0 || !g_csv) {
        Print("Error: cannot open " + CSV_PATH + "\n");
        return false;
    }
    fseek(g_csv, 0, SEEK_END);
    if (ftell(g_csv) == 0)
        fprintf(g_csv, "time,t_ms,conn,client_id,req_id,cmd,key,status,detail,bytes_in,bytes_out,proc_us,total_us,"
            "entries,active_conns\n");
    return true;
}


// Text that came from the network must not break the CSV: shortened, no commas, quotes or control characters.
static std::string Clean(std::string s) {
    s = s.substr(0, 40);
    for (char& c : s)
        if (c == ',' || c == '"' || (unsigned char)c < 0x20) c = '_';
    return s;
}


// One row per request. t_ms: when the request had been received (from the server start), proc_us: time inside
// Execute(), total_us: request received .. response sent, detail: NEW / UPDATED for PUT,
// entries: table size after the operation.
static void CsvRow(int conn, const Reply& r, double tRecv, long long procUs, long long totalUs,
    size_t bytesIn, size_t bytesOut) {
    if (!g_csv) return;
    char t[32];
    sprintf_s(t, "%.3f", tRecv - g_startMs);
    std::string row = MakeTime() + "," + t + "," + std::to_string(conn) + "," + std::to_string(r.clientId) + "," +
        std::to_string(r.reqId) + "," + Clean(r.cmd) + "," + Clean(r.key) + "," + r.status + "," +
        (r.cmd == "PUT" ? Clean(r.data) : "") + "," + std::to_string(bytesIn) + "," + std::to_string(bytesOut) + "," +
        std::to_string(procUs) + "," + std::to_string(totalUs) + "," + std::to_string(r.entries) + "," +
        std::to_string(g_active.load());

    static int rows = 0;
    std::lock_guard<std::mutex> lock(g_csvMx);
    if (!g_csv) return;
    fprintf(g_csv, "%s\n", row.c_str());
    if (++rows % 64 == 0) fflush(g_csv);
}


// ---------- client connection ----------

static void ClientThread(SOCKET sock, int id, std::string peer) {
    std::string tag = "conn=" + std::to_string(id) + " peer=" + peer;
    int clientId = 0, served = 0;
    std::string reason;

    while (true) {
        std::string payload;
        int rc = RecvMsg(sock, payload);
        double t0 = NowMs();            // the request has been received completely
        if (rc == R_BADLEN) {           // message boundaries are lost: answer with an error and close
            Reply bad;
            bad.cmd = "-";
            bad.status = "BAD_LENGTH";
            bad.clientId = clientId;
            std::string resp = std::to_string(PROTOCOL_VERSION) + " 0 BAD_LENGTH";
            SendMsg(sock, resp);
            g_requests++;
            g_rejected++;
            CsvRow(id, bad, t0, 0, (long long)((NowMs() - t0) * 1000), 4, 4 + resp.size());
        }
        if (rc != R_OK) { reason = Describe(rc); break; }

        Reply r = Execute(payload);
        long long procUs = (long long)((NowMs() - t0) * 1000);
        if (r.clientId > 0) clientId = r.clientId;

        std::string resp = std::to_string(PROTOCOL_VERSION) + " " + std::to_string(r.reqId) + " " + r.status;
        if (!r.data.empty()) resp += " " + r.data;
        if (!SendMsg(sock, resp)) { reason = "send error"; break; }

        served++;
        g_requests++;
        if (r.status != "OK" && r.status != "NOT_FOUND") g_rejected++;      // FULL, BAD_*, UNKNOWN_CMD
        CsvRow(id, r, t0, procUs, (long long)((NowMs() - t0) * 1000), 4 + payload.size(), 4 + resp.size());
        if (VERBOSE)
            Log("REQUEST " + tag + " client=" + std::to_string(r.clientId) + " cmd=" + (r.cmd.empty() ? "?" : r.cmd) +
                " reqId=" + std::to_string(r.reqId) + " size=" + std::to_string(payload.size()) + " -> " + r.status);
    }

    shutdown(sock, SD_BOTH);
    closesocket(sock);
    Log("CLIENT_DISCONNECTED " + tag + " id=" + std::to_string(clientId) +
        " requests=" + std::to_string(served) + " reason=" + reason);
    g_active--;
}


// ---------- server ----------

static BOOL WINAPI CtrlHandler(DWORD type) {
    if (type == CTRL_C_EVENT || type == CTRL_BREAK_EVENT || type == CTRL_CLOSE_EVENT) {
        g_stop = 1;
        return TRUE;
    }
    return FALSE;
}


static SOCKET CreateListener() {
    SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) {
        PrintError("socket", WSAGetLastError());
        return s;
    }
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((u_short)PORT);
    if (inet_pton(AF_INET, HOST.c_str(), &addr.sin_addr) != 1)
        Print("Error: invalid IPv4 address " + HOST + "\n");
    else if (bind(s, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR)
        PrintError("bind", WSAGetLastError());
    else if (listen(s, SOMAXCONN) == SOCKET_ERROR)
        PrintError("listen", WSAGetLastError());
    else
        return s;
    closesocket(s);
    return INVALID_SOCKET;
}


static int RunServer() {
    WSADATA wsa;
    int rc = WSAStartup(MAKEWORD(2, 2), &wsa);
    if (rc != 0) {
        PrintError("WSAStartup", rc);
        return 1;
    }
    SOCKET listener = CreateListener();
    if (listener == INVALID_SOCKET) {
        WSACleanup();
        return 1;
    }

    g_startMs = NowMs();
    if (!CSV_PATH.empty() && !OpenCsv()) {
        closesocket(listener);
        WSACleanup();
        return 1;
    }
    Log("SERVER START host=" + HOST + " port=" + std::to_string(PORT) + " timeout-ms=" + std::to_string(TIMEOUT_MS) +
        " csv=" + (CSV_PATH.empty() ? "-" : CSV_PATH) + " (Ctrl+C to stop)");

    while (!g_stop) {
        fd_set rd;
        FD_ZERO(&rd);
        FD_SET(listener, &rd);
        timeval tv = { 0, POLL_MS * 1000 };
        int r = select(0, &rd, NULL, NULL, &tv);    // wake up regularly to check g_stop
        if (r == SOCKET_ERROR) {
            PrintError("select", WSAGetLastError());
            break;
        }
        if (r == 0) continue;

        sockaddr_in peer;
        int plen = sizeof(peer);
        SOCKET s = accept(listener, (sockaddr*)&peer, &plen);
        if (s == INVALID_SOCKET) {
            PrintError("accept", WSAGetLastError());
            continue;
        }
        BOOL nodelay = TRUE;    // small request/response messages: do not wait for Nagle
        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (const char*)&nodelay, sizeof(nodelay));

        char ip[INET_ADDRSTRLEN] = "?";
        inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
        std::string peerName = std::string(ip) + ":" + std::to_string(ntohs(peer.sin_port));
        int id = ++g_connections;

        g_active++;
        try {
            std::thread(ClientThread, s, id, peerName).detach();
        }
        catch (const std::exception& e) {
            Print(std::string("Error: cannot create thread: ") + e.what() + "\n");
            g_active--;
            closesocket(s);
            continue;
        }
        Log("CLIENT_CONNECTED conn=" + std::to_string(id) + " peer=" + peerName);
    }

    g_stop = 1;
    closesocket(listener);
    for (int i = 0; i < 100 && g_active > 0; i++) Sleep(50);    // handlers notice g_stop within POLL_MS
    if (g_active > 0) Print("Warning: " + std::to_string(g_active.load()) + " client thread(s) still running\n");
    {
        std::lock_guard<std::mutex> lock(g_csvMx);
        if (g_csv) fclose(g_csv);
        g_csv = NULL;
    }

    Log("SERVER STOP connections=" + std::to_string(g_connections.load()) +
        " requests=" + std::to_string(g_requests.load()) + " rejected=" + std::to_string(g_rejected.load()));
    WSACleanup();
    return 0;
}


int main(int argc, char* argv[]) {
    if ((argc - 1) % 2 != 0) {
        Print("Error: every parameter needs a value\n");
        return 2;
    }
    for (int i = 1; i < argc; i += 2) {
        std::string key = argv[i], val = argv[i + 1];
        int n = atoi(val.c_str());
        if (key == "--host") HOST = val;
        else if (key == "--port") PORT = n;
        else if (key == "--timeout-ms") TIMEOUT_MS = n;
        else if (key == "--csv") CSV_PATH = val;
        else if (key == "--verbose") VERBOSE = (n != 0);
        else {
            Print("Error: unknown parameter " + key + "\n");
            return 2;
        }
    }
    if (PORT < 1 || PORT > 65535 || TIMEOUT_MS < 1) {
        Print("Error: port must be 1..65535 and timeout-ms must be positive\n");
        return 2;
    }
    SetConsoleCtrlHandler(CtrlHandler, TRUE);
    return RunServer();
}