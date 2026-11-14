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
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#pragma comment(lib, "ws2_32.lib")

const int STUDENT_ID = 82;
const int PROTOCOL_VERSION = 1;
const int MAX_ENTRIES = 1000;
const int MAX_MSG = 65536;
const int MAX_CLIENTS = 60;
const int POLL_MS = 100;

enum { R_OK = 1, R_CLOSED = 0, R_ERROR = -1, R_TIMEOUT = -2, R_BADLEN = -4 };

static std::string HOST = "127.0.0.1";
static int PORT = 20000 + STUDENT_ID;
static int CLIENTS = 1;
static int OPS = 100;                   // operations per client
static int KEYS = 20;                   // own keys per client
static int TIMEOUT_MS = 5000;           // max wait for a response
static int DELAY_MS = 20;               // pause between requests of one client
static int SEED = 1000 + STUDENT_ID * 97;
static std::string CMD;                 // single raw request mode (manual error checks)

static std::mutex g_printMx;
static std::atomic<int> g_ready(0);     // workers that have connected (or failed to)
static std::atomic<int> g_go(0);        // start all workers at once
static std::atomic<int> g_cancel(0);

struct Worker {
    int id;
    SOCKET sock;
    int sent, received, ok, full, errors, timeouts, violations;
    int hotSeq;                         // confirmed writes of "hot"
    int ownPresent;                     // own keys that should be in the table at the end
    bool aborted;
    long long bytesSent, bytesRecv;
    double latSum, latMax, connectMs;
};

struct Rep {
    std::string status, data;
};

// ---------- helpers ----------

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

static std::string F(double v) {
    char b[32];
    sprintf_s(b, "%.3f", v);
    return b;
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

// ---------- network ----------

static SOCKET Connect() {
    SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) {
        PrintError("socket", WSAGetLastError());
        return s;
    }
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((u_short)PORT);
    if (inet_pton(AF_INET, HOST.c_str(), &addr.sin_addr) != 1) {
        Print("Error: invalid IPv4 address " + HOST + "\n");
    }
    else if (connect(s, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        PrintError("connect", WSAGetLastError());
    }
    else {
        BOOL nodelay = TRUE;    // small request/response messages: do not wait for Nagle
        setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (const char*)&nodelay, sizeof(nodelay));
        return s;
    }
    closesocket(s);
    return INVALID_SOCKET;
}

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

// Reads exactly len bytes; handles partial recv() and enforces the response timeout.
static int RecvAll(SOCKET s, char* buf, int len) {
    int got = 0, waited = 0;
    while (got < len) {
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
        if (n == 0) return got == 0 ? R_CLOSED : R_ERROR;
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
    case R_CLOSED:  return "connection closed by server";
    case R_TIMEOUT: return "response timeout";
    case R_BADLEN:  return "invalid response length";
    default:        return "network or protocol error";
    }
}

// One request/response exchange. After a failure the connection must not be used again.
// RTT is measured from just before send() to the moment the whole response has been read,
// so it includes the network and the work done by the server.
static int Transact(Worker* w, int reqId, const std::string& body, Rep& rep) {
    std::string payload = std::to_string(PROTOCOL_VERSION) + " " + std::to_string(reqId) + " " +
        std::to_string(w->id) + " " + body;

    double t0 = NowMs();
    w->sent++;
    if (!SendMsg(w->sock, payload)) {
        w->errors++;
        return R_ERROR;
    }
    w->bytesSent += 4 + (long long)payload.size();

    std::string resp;
    int rc = RecvMsg(w->sock, resp);
    if (rc != R_OK) {
        (rc == R_TIMEOUT ? w->timeouts : w->errors)++;
        Print("Error: client " + std::to_string(w->id) + " reqId=" + std::to_string(reqId) + ": " + Describe(rc) + "\n");
        return rc;
    }

    double rtt = NowMs() - t0;
    w->received++;
    w->bytesRecv += 4 + (long long)resp.size();
    w->latSum += rtt;
    w->latMax = std::max(w->latMax, rtt);

    // "<ver> <reqId> <status> [data]"
    std::istringstream in(resp);
    int ver = 0, rid = -1;
    rep.status.clear();
    if (!(in >> ver >> rid >> rep.status) || ver != PROTOCOL_VERSION || rid != reqId) {
        w->errors++;
        Print("Error: client " + std::to_string(w->id) + " malformed response '" + resp + "'\n");
        return R_ERROR;
    }
    std::getline(in >> std::ws, rep.data);
    return R_OK;
}

// ---------- one client ----------

static void WorkerThread(Worker* w) {
    const std::string id = std::to_string(w->id);

    double t0 = NowMs();
    w->sock = Connect();
    w->connectMs = NowMs() - t0;
    g_ready++;                                  // tell main: connected, or failed
    if (w->sock == INVALID_SOCKET) {
        w->aborted = true;
        return;
    }
    while (!g_go) Sleep(1);                     // all clients start together
    if (g_cancel) {
        closesocket(w->sock);
        w->aborted = true;
        return;
    }

    std::mt19937 rng((unsigned)(SEED + w->id));
    std::vector<std::string> mirror(KEYS);      // what this client last wrote to each own key
    std::vector<char> present(KEYS, 0);
    std::vector<int> lastSeen(CLIENTS + 1, 0);  // last "hot" sequence number seen per writer
    bool seenHot = false;
    int seqHot = 0, reqId = 0, done = 0;

    auto Bad = [&](const std::string& what) {
        w->violations++;
        Print("VIOLATION client=" + id + ": " + what + "\n");
        };
    auto Expect = [&](bool good, const std::string& what) {
        if (good) w->ok++;
        else Bad(what);
        };

    for (int i = 0; i < OPS; i++) {
        int dice = (int)(rng() % 100);
        int k = (int)(rng() % KEYS);
        std::string key = "c" + id + "_k" + std::to_string(k);
        Rep rep;

        if (dice < 15) {                                    // PUT hot: concurrent update of one record
            if (Transact(w, ++reqId, "PUT hot c" + id + ":" + std::to_string(seqHot + 1), rep) != R_OK) break;
            if (rep.status == "OK") { seqHot++; w->ok++; }
            else if (rep.status == "FULL") w->full++;
            else Bad("PUT hot returned " + rep.status);
        }
        else if (dice < 30) {                               // GET hot
            if (Transact(w, ++reqId, "GET hot", rep) != R_OK) break;
            if (rep.status == "OK") {
                int j = 0, s = 0;
                bool parsed = sscanf_s(rep.data.c_str(), "c%d:%d", &j, &s) == 2 && j >= 1 && j <= CLIENTS && s >= 1;
                // a writer's sequence never goes back in time, and nobody sees its own future writes
                Expect(parsed && s >= lastSeen[j] && (j != w->id || s <= seqHot), "GET hot returned '" + rep.data + "'");
                if (parsed) {
                    seenHot = true;
                    lastSeen[j] = std::max(lastSeen[j], s);
                }
            }
            else {      // nobody deletes "hot", so NOT_FOUND is valid only before the first write
                Expect(rep.status == "NOT_FOUND" && seqHot == 0 && !seenHot, "GET hot returned " + rep.status);
            }
        }
        else if (dice < 55) {                               // PUT own key (new, or update of an existing one)
            std::string value = "v" + id + "_" + std::to_string(i);
            if (Transact(w, ++reqId, "PUT " + key + " " + value, rep) != R_OK) break;
            if (rep.status == "OK") {
                Expect(rep.data == (present[k] ? "UPDATED" : "NEW"), "PUT " + key + " answered " + rep.data);
                mirror[k] = value;
                present[k] = 1;
            }
            else if (rep.status == "FULL") {
                Expect(!present[k], "PUT " + key + " rejected as FULL but the key exists");
                w->full++;
            }
            else Bad("PUT " + key + " returned " + rep.status);
        }
        else if (dice < 80) {                               // GET own key (existing or missing)
            if (Transact(w, ++reqId, "GET " + key, rep) != R_OK) break;
            if (present[k]) Expect(rep.status == "OK" && rep.data == mirror[k], "GET " + key + " returned " + rep.status + " '" + rep.data + "'");
            else Expect(rep.status == "NOT_FOUND", "GET " + key + " (missing) returned " + rep.status);
        }
        else if (dice < 90) {                               // DELETE own key
            if (Transact(w, ++reqId, "DELETE " + key, rep) != R_OK) break;
            Expect(rep.status == (present[k] ? "OK" : "NOT_FOUND"), "DELETE " + key + " returned " + rep.status);
            present[k] = 0;
        }
        else if (dice < 95) {                               // COUNT
            if (Transact(w, ++reqId, "COUNT", rep) != R_OK) break;
            int n = atoi(rep.data.c_str());
            int own = (int)std::count(present.begin(), present.end(), 1);
            Expect(rep.status == "OK" && n >= own && n <= MAX_ENTRIES, "COUNT returned " + rep.status + " " + rep.data);
        }
        else {                                              // LIST
            if (Transact(w, ++reqId, "LIST", rep) != R_OK) break;
            std::set<std::string> keys;
            std::istringstream in(rep.data);
            for (std::string tok; in >> tok;) keys.insert(tok);
            bool good = rep.status == "OK";
            for (int j = 0; j < KEYS && good; j++)
                if (present[j] && !keys.count("c" + id + "_k" + std::to_string(j))) good = false;
            Expect(good, "LIST lost one of the client's own keys");
        }
        done++;
        if (DELAY_MS > 0) Sleep(DELAY_MS);
    }

    w->aborted = (done < OPS);
    w->hotSeq = seqHot;
    w->ownPresent = (int)std::count(present.begin(), present.end(), 1);
    shutdown(w->sock, SD_BOTH);
    closesocket(w->sock);
}

// ---------- experiment ----------

// Clears the table before an experiment: LIST, then DELETE every key.
static bool ResetTable() {
    Worker w = {};
    w.id = 0;
    if ((w.sock = Connect()) == INVALID_SOCKET) return false;

    Rep rep;
    int reqId = 0;
    bool ok = Transact(&w, ++reqId, "LIST", rep) == R_OK && rep.status == "OK";
    std::istringstream in(rep.data);
    for (std::string key; ok && (in >> key);) ok = Transact(&w, ++reqId, "DELETE " + key, rep) == R_OK;
    closesocket(w.sock);
    return ok;
}

// After all clients are done the table must hold exactly what they wrote:
// "hot" equals the last write of the writer whose value it holds, COUNT equals the sum of own keys (+1 for hot).
static bool FinalCheck(const std::vector<Worker>& workers) {
    int fullTotal = 0, expected = 0;
    bool anyHot = false;
    for (const Worker& x : workers) {
        fullTotal += x.full;
        expected += x.ownPresent;
        anyHot |= x.hotSeq > 0;
    }
    if (anyHot) expected++;

    Worker w = {};
    w.id = 0;
    if ((w.sock = Connect()) == INVALID_SOCKET) return false;

    bool good = true;
    Rep rep;
    if (Transact(&w, 1, "GET hot", rep) != R_OK) {
        good = false;
    }
    else if (!anyHot) {
        good = (rep.status == "NOT_FOUND");
    }
    else {
        int j = 0, s = 0;
        good = rep.status == "OK" && sscanf_s(rep.data.c_str(), "c%d:%d", &j, &s) == 2 &&
            j >= 1 && j <= CLIENTS && s == workers[j - 1].hotSeq;
        if (!good) Print("FINAL CHECK: hot = '" + rep.data + "' is not the last write of any client\n");
    }

    if (Transact(&w, 2, "COUNT", rep) != R_OK) {
        good = false;
    }
    else if (fullTotal == 0 && !(rep.status == "OK" && atoi(rep.data.c_str()) == expected)) {
        good = false;       // with FULL answers the exact size is not known, so it is checked only without them
        Print("FINAL CHECK: COUNT = " + rep.data + ", expected " + std::to_string(expected) + "\n");
    }
    closesocket(w.sock);
    return good;
}

static int RunLoad() {
    if (!ResetTable()) {
        Print("Error: cannot reset the table on the server\n");
        return 1;
    }

    std::vector<Worker> workers(CLIENTS);
    std::vector<std::thread> threads;
    for (int i = 0; i < CLIENTS; i++) {         // start K clients
        workers[i] = Worker();
        workers[i].id = i + 1;
        workers[i].sock = INVALID_SOCKET;
        threads.emplace_back(WorkerThread, &workers[i]);
    }

    while (g_ready < CLIENTS) Sleep(1);
    for (const Worker& w : workers)
        if (w.sock == INVALID_SOCKET) g_cancel = 1;

    double t0 = NowMs();
    g_go = 1;
    for (std::thread& t : threads) t.join();
    double totalMs = NowMs() - t0;

    if (g_cancel) {
        Print("Error: not all clients could connect, experiment cancelled\n");
        return 1;
    }

    Worker sum = {};            // totals over all clients
    bool aborted = false;
    for (const Worker& w : workers) {
        sum.sent += w.sent; sum.received += w.received; sum.ok += w.ok; sum.full += w.full;
        sum.errors += w.errors; sum.timeouts += w.timeouts; sum.violations += w.violations;
        sum.bytesSent += w.bytesSent; sum.bytesRecv += w.bytesRecv;
        sum.latSum += w.latSum; sum.connectMs += w.connectMs;
        sum.latMax = std::max(sum.latMax, w.latMax);
        aborted |= w.aborted;
    }

    bool finalOk = false;
    if (aborted) Print("Final check skipped: some clients stopped early\n");
    else finalOk = FinalCheck(workers);

    double throughput = totalMs > 0 ? sum.received / (totalMs / 1000.0) : 0.0;
    auto N = [](long long v) { return std::to_string(v); };
    const std::pair<const char*, std::string> cols[] = {
        {"clients", N(CLIENTS)}, {"ops_per_client", N(OPS)}, {"keys", N(KEYS)}, {"seed", N(SEED)},
        {"total_ms", F(totalMs)}, {"throughput_ops_s", F(throughput)},
        {"sent", N(sum.sent)}, {"received", N(sum.received)}, {"ok", N(sum.ok)}, {"full", N(sum.full)},
        {"errors", N(sum.errors)}, {"timeouts", N(sum.timeouts)}, {"violations", N(sum.violations)},
        {"bytes_sent", N(sum.bytesSent)}, {"bytes_received", N(sum.bytesRecv)},
        {"avg_connect_ms", F(sum.connectMs / CLIENTS)},
        {"avg_rtt_ms", F(sum.received ? sum.latSum / sum.received : 0.0)}, {"max_rtt_ms", F(sum.latMax)},
        {"final_check_ok", finalOk ? "1" : "0"} };

    std::string text = "\n=== RESULT ===\n";
    for (const auto& col : cols) text += std::string(col.first) + "=" + col.second + "\n";
    Print(text);
    return (sum.violations == 0 && sum.errors == 0 && sum.timeouts == 0 && !aborted && finalOk) ? 0 : 1;
}

// One raw request, for manual checks (unknown command, bad arguments, missing key, ...)
static int RunSingle() {
    Worker w = {};
    w.id = 0;
    if ((w.sock = Connect()) == INVALID_SOCKET) return 1;

    Rep rep;
    int rc = Transact(&w, 1, CMD, rep);
    if (rc == R_OK) Print("status=" + rep.status + (rep.data.empty() ? "" : " data=" + rep.data) + "\n");
    closesocket(w.sock);
    return rc == R_OK ? 0 : 1;
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
        else if (key == "--clients") CLIENTS = n;
        else if (key == "--ops") OPS = n;
        else if (key == "--keys") KEYS = n;
        else if (key == "--timeout-ms") TIMEOUT_MS = n;
        else if (key == "--delay-ms") DELAY_MS = n;
        else if (key == "--seed") SEED = n;
        else if (key == "--cmd") CMD = val;
        else {
            Print("Error: unknown parameter " + key + "\n");
            return 2;
        }
    }
    if (PORT < 1 || PORT > 65535 || OPS < 1 || KEYS < 1 || KEYS > MAX_ENTRIES || TIMEOUT_MS < 1 || DELAY_MS < 0 ||
        CLIENTS < 1 || CLIENTS > MAX_CLIENTS) {
        Print("Error: invalid parameter value (clients 1.." + std::to_string(MAX_CLIENTS) + ", ops >= 1, keys 1.." +
            std::to_string(MAX_ENTRIES) + ", port 1..65535, timeout-ms >= 1)\n");
        return 2;
    }

    WSADATA wsa;
    int rc = WSAStartup(MAKEWORD(2, 2), &wsa);
    if (rc != 0) {
        PrintError("WSAStartup", rc);
        return 1;
    }
    int code = CMD.empty() ? RunLoad() : RunSingle();
    WSACleanup();
    return code;
}