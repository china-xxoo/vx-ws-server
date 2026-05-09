const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const TOKEN = process.env.VX_NOTIFY_TOKEN || "change-this-token";

const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
    return;
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("vx websocket notifier running");
});

const wss = new WebSocket.Server({ server });

const clients = new Map();

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcast(data, filterFn) {
  for (const [, client] of clients) {
    if (!client.authed) continue;
    if (filterFn && !filterFn(client)) continue;
    send(client.ws, data);
  }
}

wss.on("connection", (ws) => {
  const id = Math.random().toString(36).slice(2);

  clients.set(id, {
    ws,
    authed: false,
    rooms: new Set()
  });

  send(ws, {
    type: "hello",
    message: "connected"
  });

  ws.on("message", (raw) => {
    let msg;

    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    const client = clients.get(id);
    if (!client) return;

    if (msg.type === "auth") {
      if (msg.token !== TOKEN) {
        send(ws, {
          type: "error",
          message: "bad token"
        });

        ws.close();
        return;
      }

      client.authed = true;

      send(ws, {
        type: "ready"
      });

      return;
    }

    if (!client.authed) {
      send(ws, {
        type: "error",
        message: "not authed"
      });

      return;
    }

    if (msg.type === "joinRoom" && msg.roomId) {
      client.rooms.add(String(msg.roomId));

      send(ws, {
        type: "joinedRoom",
        roomId: String(msg.roomId)
      });

      return;
    }

    if (msg.type === "leaveRoom" && msg.roomId) {
      client.rooms.delete(String(msg.roomId));
      return;
    }

    if (msg.type === "messageUpdate" && msg.roomId) {
      const roomId = String(msg.roomId);

      broadcast(
        {
          type: "messageUpdate",
          roomId,
          time: Date.now()
        },
        (client) => client.rooms.has(roomId)
      );

      return;
    }

    if (msg.type === "roomsUpdate") {
      broadcast({
        type: "roomsUpdate",
        time: Date.now()
      });

      return;
    }
  });

  ws.on("close", () => {
    clients.delete(id);
  });

  ws.on("error", () => {
    clients.delete(id);
  });
});

server.listen(PORT, () => {
  console.log("vx websocket notifier running on port " + PORT);
});
