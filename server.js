const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT) || 10000;
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS) || 8;
const GAME_VERSION = process.env.GAME_VERSION || "";

const MSG = {
  HOST: 1,
  JOIN: 2,
  PACKET: 3,
  HEARTBEAT: 4,
  ROOM: 10,
  JOINED: 11,
  ERROR: 12,
  TO_HOST: 13,
  TO_CLIENT: 14,
  PEER_JOIN: 15,
  PEER_LEAVE: 16,
  HOST_LEFT: 17,
  HEARTBEAT_ACK: 18,
};

const rooms = new Map();
let nextClientId = 1;

function send(ws, type, payload = Buffer.alloc(0)) {
  if (ws.readyState !== ws.OPEN) return;
  const buf = Buffer.allocUnsafe(1 + payload.length);
  buf[0] = type;
  if (payload.length) payload.copy(buf, 1);
  ws.send(buf, { binary: true });
}

function sendError(ws, message) {
  send(ws, MSG.ERROR, Buffer.from(String(message), "utf8"));
}

function makeRoomCode() {
  for (let i = 0; i < 20; i++) {
    const code = String(100000 + crypto.randomInt(900000));
    if (!rooms.has(code)) return code;
  }
  throw new Error("Could not allocate a room code");
}

function parseText(buf) {
  return buf.toString("utf8").replace(/\0/g, "").trim();
}

function destroyRoom(code, reason) {
  const room = rooms.get(code);
  if (!room) return;
  rooms.delete(code);
  for (const client of room.clients.values()) {
    try {
      send(client.ws, MSG.HOST_LEFT);
      client.ws.close(4000, reason || "host left");
    } catch {
      // ignore
    }
  }
}

function removeClient(room, clientId, closeSocket) {
  const client = room.clients.get(clientId);
  if (!client) return;
  room.clients.delete(clientId);
  if (closeSocket) {
    try {
      client.ws.close();
    } catch {
      // ignore
    }
  }
  if (room.host && room.host.readyState === room.host.OPEN) {
    const payload = Buffer.allocUnsafe(4);
    payload.writeInt32LE(clientId, 0);
    send(room.host, MSG.PEER_LEAVE, payload);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === "/health" || req.url === "/") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        ok: true,
        service: "game-render-relay",
        rooms: rooms.size,
      })
    );
    return;
  }
  res.writeHead(404);
  res.end("not found");
});

const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", (ws) => {
  ws.binaryType = "nodebuffer";
  ws.isAlive = true;
  ws.role = null;
  ws.roomCode = null;
  ws.clientId = 0;

  ws.on("pong", () => {
    ws.isAlive = true;
  });

  ws.on("message", (data) => {
    if (!data || data.length < 1) return;
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    const type = buf[0];
    const payload = buf.subarray(1);

    if (type === MSG.HEARTBEAT) {
      send(ws, MSG.HEARTBEAT_ACK);
      return;
    }

    if (type === MSG.HOST) {
      if (ws.role) {
        sendError(ws, "Already in a room.");
        return;
      }
      const version = parseText(payload);
      if (GAME_VERSION && version && version !== GAME_VERSION) {
        sendError(ws, "Version mismatch.");
        ws.close();
        return;
      }
      const code = makeRoomCode();
      rooms.set(code, {
        host: ws,
        clients: new Map(),
        version,
      });
      ws.role = "host";
      ws.roomCode = code;
      send(ws, MSG.ROOM, Buffer.from(code, "utf8"));
      console.log(`room ${code} created`);
      return;
    }

    if (type === MSG.JOIN) {
      if (ws.role) {
        sendError(ws, "Already in a room.");
        return;
      }
      const text = parseText(payload);
      const [code, version] = text.split("\n");
      const room = rooms.get(code);
      if (!room) {
        sendError(ws, "Room not found.");
        return;
      }
      if (GAME_VERSION && version && version !== GAME_VERSION) {
        sendError(ws, "Version mismatch.");
        return;
      }
      if (room.version && version && room.version !== version) {
        sendError(ws, "Version mismatch.");
        return;
      }
      if (room.clients.size + 1 >= MAX_PLAYERS) {
        sendError(ws, "Room is full.");
        return;
      }
      const clientId = nextClientId++;
      ws.role = "client";
      ws.roomCode = code;
      ws.clientId = clientId;
      room.clients.set(clientId, { ws, clientId });
      const joined = Buffer.allocUnsafe(4);
      joined.writeInt32LE(clientId, 0);
      send(ws, MSG.JOINED, joined);
      if (room.host && room.host.readyState === room.host.OPEN) {
        send(room.host, MSG.PEER_JOIN, joined);
      }
      console.log(`client ${clientId} joined room ${code}`);
      return;
    }

    if (type === MSG.PACKET) {
      const room = rooms.get(ws.roomCode);
      if (!room) return;
      if (ws.role === "host") {
        if (payload.length < 5) return;
        const targetId = payload.readInt32LE(0);
        const body = payload.subarray(4);
        const target = room.clients.get(targetId);
        if (target) send(target.ws, MSG.TO_CLIENT, body);
        return;
      }
      if (ws.role === "client" && room.host) {
        const wrapped = Buffer.allocUnsafe(4 + payload.length);
        wrapped.writeInt32LE(ws.clientId, 0);
        payload.copy(wrapped, 4);
        send(room.host, MSG.TO_HOST, wrapped);
      }
    }
  });

  ws.on("close", () => {
    if (ws.role === "host" && ws.roomCode) {
      console.log(`host left room ${ws.roomCode}`);
      destroyRoom(ws.roomCode, "host left");
    } else if (ws.role === "client" && ws.roomCode) {
      const room = rooms.get(ws.roomCode);
      if (room) removeClient(room, ws.clientId, false);
      console.log(`client ${ws.clientId} left room ${ws.roomCode}`);
    }
  });

  ws.on("error", (err) => {
    console.error("socket error", err.message);
  });
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 25000);

wss.on("close", () => clearInterval(heartbeat));

server.listen(PORT, "0.0.0.0", () => {
  console.log(`game relay listening on ${PORT}`);
});
