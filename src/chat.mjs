// This is the Edge Chat Demo Worker, built using Durable Objects!

// ===============================
// Introduction to Modules
// ===============================
//
// The first thing you might notice, if you are familiar with the Workers platform, is that this
// Worker is written differently from others you may have seen. It even has a different file
// extension. The `mjs` extension means this JavaScript is an ES Module, which, among other things,
// means it has imports and exports. Unlike other Workers, this code doesn't use
// `addEventListener("fetch", handler)` to register its main HTTP handler; instead, it _exports_
// a handler, as we'll see below.
//
// This is a new way of writing Workers that we expect to introduce more broadly in the future. We
// like this syntax because it is *composable*: You can take two workers written this way and
// merge them into one worker, by importing the two Workers' exported handlers yourself, and then
// exporting a new handler that call into the other Workers as appropriate.
//
// This new syntax is required when using Durable Objects, because your Durable Objects are
// implemented by classes, and those classes need to be exported. The new syntax can be used for
// writing regular Workers (without Durable Objects) too, but for now, you must be in the Durable
// Objects beta to be able to use the new syntax, while we work out the quirks.
//
// To see an example configuration for uploading module-based Workers, check out the wrangler.toml
// file or one of our Durable Object templates for Wrangler:
//   * https://github.com/cloudflare/durable-objects-template
//   * https://github.com/cloudflare/durable-objects-rollup-esm
//   * https://github.com/cloudflare/durable-objects-webpack-commonjs

// ===============================
// Required Environment
// ===============================
//
// This worker, when deployed, must be configured with two environment bindings:
// * rooms: A Durable Object namespace binding mapped to the ChatRoom class.
// * limiters: A Durable Object namespace binding mapped to the RateLimiter class.
//
// Incidentally, in pre-modules Workers syntax, "bindings" (like KV bindings, secrets, etc.)
// appeared in your script as global variables, but in the new modules syntax, this is no longer
// the case. Instead, bindings are now delivered in an "environment object" when an event handler
// (or Durable Object class constructor) is called. Look for the variable `env` below.
//
// We made this change, again, for composability: The global scope is global, but if you want to
// call into existing code that has different environment requirements, then you need to be able
// to pass the environment as a parameter instead.
//
// Once again, see the wrangler.toml file to understand how the environment is configured.

// =======================================================================================
// The regular Worker part...
//
// This section of the code implements a normal Worker that receives HTTP requests from external
// clients. This part is stateless.

// With the introduction of modules, we're experimenting with allowing text/data blobs to be
// uploaded and exposed as synthetic modules. In wrangler.toml we specify a rule that files ending
// in .html should be uploaded as "Data", equivalent to content-type `application/octet-stream`.
// So when we import it as `HTML` here, we get the HTML content as an `ArrayBuffer`. This lets us
// serve our app's static asset without relying on any separate storage. (However, the space
// available for assets served this way is very limited; larger sites should continue to use Workers
// KV to serve assets.)
import HTML from "./chat.html";
import ADMIN_HTML from "./chat-admin.html";
import CSS from "./chat.css";
import ExcelJS from "exceljs";

// `handleErrors()` is a little utility function that can wrap an HTTP request handler in a
// try/catch and return errors to the client. You probably wouldn't want to use this in production
// code but it is convenient when debugging and iterating.
async function handleErrors(request, func) {
  try {
    return await func();
  } catch (err) {
    if (request.headers.get("Upgrade") == "websocket") {
      // Annoyingly, if we return an HTTP error in response to a WebSocket request, Chrome devtools
      // won't show us the response body! So... let's send a WebSocket response with an error
      // frame instead.
      let pair = new WebSocketPair();
      pair[1].accept();
      pair[1].send(JSON.stringify({error: err.stack}));
      pair[1].close(1011, "Uncaught exception during session setup");
      return new Response(null, { status: 101, webSocket: pair[0] });
    } else {
      return new Response(err.stack, {status: 500});
    }
  }
}

// In modules-syntax workers, we use `export default` to export our script's main event handlers.
// Here, we export one handler, `fetch`, for receiving HTTP requests. In pre-modules workers, the
// fetch handler was registered using `addEventHandler("fetch", event => { ... })`; this is just
// new syntax for essentially the same thing.
//
// `fetch` isn't the only handler. If your worker runs on a Cron schedule, it will receive calls
// to a handler named `scheduled`, which should be exported here in a similar way. We will be
// adding other handlers for other types of events over time.
export default {
  async fetch(request, env) {
    return await handleErrors(request, async () => {
      // We have received an HTTP request! Parse the URL and route the request.

      let url = new URL(request.url);
      let path = url.pathname.slice(1).split('/');

      if (!path[0]) {
        // Serve our HTML at the root path.
        return new Response(HTML, {headers: {"Content-Type": "text/html;charset=UTF-8"}});
      }

      switch (path[0]) {
        case "chat.css": {
          return new Response(CSS, { headers: { "Content-Type": "text/css;charset=UTF-8", "Cache-Control": "public, max-age=3600" } });
        }
        case "admin": {
          // Serve admin chat page (requires admin session).
          let session = await validateSession(request, env);
          if (!session || !session.admin) {
            return new Response(null, { status: 302, headers: { "Location": "/" } });
          }
          return new Response(ADMIN_HTML, {headers: {"Content-Type": "text/html;charset=UTF-8"}});
        }
        case "login": {
          // Token-based login: GET /login?token=<token>
          if (request.method !== "GET") {
            return new Response("Method not allowed", { status: 405 });
          }

          let token = url.searchParams.get("token");
          if (!token) {
            return new Response("Missing token.", { status: 400 });
          }

          // Look up the token in the ALLOWED_USERS secret (JSON: {token: {email, admin}}).
          let allowedUsers;
          try {
            allowedUsers = JSON.parse(env.ALLOWED_USERS);
          } catch {
            return new Response("Server misconfiguration.", { status: 500 });
          }

          let entry = allowedUsers[token];
          if (!entry) {
            return new Response("Invalid or expired login link.", { status: 403 });
          }

          let email = entry.email.toLowerCase().trim();
          let isAdmin = !!entry.admin;

          // Create a session via the AuthSession DO
          let id = env.authSessions.idFromName(email);
          let stub = env.authSessions.get(id);

          let resp = await stub.fetch(new Request("https://dummy/create-session", {
            method: "POST",
            headers: { "Content-Type": "application/json" }
          }));

          if (!resp.ok) {
            return new Response("Failed to create session.", { status: 500 });
          }

          let { token: sessionToken } = await resp.json();

          let maxAge = 30 * 24 * 60 * 60; // 30 days
          let headers = new Headers({ "Location": isAdmin ? "/admin" : "/" });
          headers.append("Set-Cookie", sessionCookie(sessionToken, maxAge));
          headers.append("Set-Cookie", `session_email=${encodeURIComponent(email)}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=${maxAge}`);
          headers.append("Set-Cookie", `session_admin=${isAdmin ? "1" : "0"}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=${maxAge}`);
          return new Response(null, { status: 302, headers });
        }

        case "api":
          // This is a request for `/api/...`, call the API handler.
          return handleApiRequest(path.slice(1), request, env);

        default:
          return new Response("Not found", {status: 404});
      }
    });
  }
}


// =======================================================================================
// Helper: parse session token from cookie header
function getSessionToken(request) {
  let cookie = request.headers.get("Cookie") || "";
  let match = cookie.match(/(?:^|;\s*)session=([a-f0-9]{64})(?:;|$)/);
  return match ? match[1] : null;
}

// Helper: create a Set-Cookie header value for the session token
function sessionCookie(token, maxAgeSecs) {
  return `session=${token}; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=${maxAgeSecs}`;
}

// Helper: escape a value for CSV (RFC 4180)
function csvEscape(val) {
  let s = String(val);
  if (s.includes(",") || s.includes("\"") || s.includes("\n") || s.includes("\r")) {
    return "\"" + s.replace(/"/g, "\"\"") + "\"";
  }
  return s;
}

// Helper: call DeepL Free API to translate text
async function translateText(text, targetLang, apiKey) {
  try {
    let resp = await fetch("https://api-free.deepl.com/v2/translate", {
      method: "POST",
      headers: {
        "Authorization": `DeepL-Auth-Key ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        text: [text],
        target_lang: targetLang,
      }),
    });
    if (!resp.ok) return null;
    let data = await resp.json();
    let t = data.translations[0];
    return { translatedText: t.text, detectedSourceLang: t.detected_source_language };
  } catch {
    return null;
  }
}

// Helper: validate session by calling AuthSession DO, returns {email, displayName} or null
async function validateSession(request, env) {
  let token = getSessionToken(request);
  if (!token) return null;

  // We need to find which email this token belongs to. We store email in a signed cookie.
  let cookie = request.headers.get("Cookie") || "";
  let emailMatch = cookie.match(/(?:^|;\s*)session_email=([^;]+)(?:;|$)/);
  if (!emailMatch) return null;

  let email = decodeURIComponent(emailMatch[1]).toLowerCase().trim();
  let id = env.authSessions.idFromName(email);
  let stub = env.authSessions.get(id);

  let resp = await stub.fetch(new Request("https://dummy/validate", {
    method: "POST",
    body: JSON.stringify({ token }),
    headers: { "Content-Type": "application/json" }
  }));

  if (!resp.ok) return null;
  let data = await resp.json();

  // Check admin cookie
  let adminMatch = cookie.match(/(?:^|;\s*)session_admin=([^;]+)(?:;|$)/);
  let admin = adminMatch ? adminMatch[1] === "1" : false;

  return { email, displayName: data.displayName, admin };
}

// =======================================================================================
// Auth API routes

async function handleAuthRequest(path, request, env) {
  switch (path[0]) {
    case "session": {
      if (request.method !== "GET") {
        return new Response("Method not allowed", { status: 405 });
      }

      let session = await validateSession(request, env);
      if (!session) {
        return new Response(JSON.stringify({ error: "Not authenticated." }), {
          status: 401, headers: { "Content-Type": "application/json" }
        });
      }

      return new Response(JSON.stringify({
        email: session.email,
        displayName: session.displayName,
        admin: session.admin
      }), {
        headers: { "Content-Type": "application/json" }
      });
    }

    case "set-name": {
      if (request.method !== "POST") {
        return new Response("Method not allowed", { status: 405 });
      }

      let session = await validateSession(request, env);
      if (!session) {
        return new Response(JSON.stringify({ error: "Not authenticated." }), {
          status: 401, headers: { "Content-Type": "application/json" }
        });
      }

      let { displayName } = await request.json();
      if (!displayName || typeof displayName !== "string" || displayName.trim().length === 0) {
        return new Response(JSON.stringify({ error: "Display name is required." }), {
          status: 400, headers: { "Content-Type": "application/json" }
        });
      }

      displayName = displayName.trim();
      if (displayName.length > 32) {
        return new Response(JSON.stringify({ error: "Display name too long." }), {
          status: 400, headers: { "Content-Type": "application/json" }
        });
      }

      let id = env.authSessions.idFromName(session.email);
      let stub = env.authSessions.get(id);

      await stub.fetch(new Request("https://dummy/set-name", {
        method: "POST",
        body: JSON.stringify({ displayName }),
        headers: { "Content-Type": "application/json" }
      }));

      return new Response(JSON.stringify({ success: true, displayName }), {
        headers: { "Content-Type": "application/json" }
      });
    }

    case "logout": {
      if (request.method !== "POST") {
        return new Response("Method not allowed", { status: 405 });
      }

      let token = getSessionToken(request);
      let cookie = request.headers.get("Cookie") || "";
      let emailMatch = cookie.match(/(?:^|;\s*)session_email=([^;]+)(?:;|$)/);

      if (token && emailMatch) {
        let email = decodeURIComponent(emailMatch[1]).toLowerCase().trim();
        let id = env.authSessions.idFromName(email);
        let stub = env.authSessions.get(id);

        await stub.fetch(new Request("https://dummy/remove-session", {
          method: "POST",
          body: JSON.stringify({ token }),
          headers: { "Content-Type": "application/json" }
        }));
      }

      let headers = new Headers({ "Content-Type": "application/json" });
      headers.append("Set-Cookie", sessionCookie("0".repeat(64), 0));
      headers.append("Set-Cookie", `session_email=; HttpOnly; Secure; SameSite=Strict; Path=/; Max-Age=0`);
      return new Response(JSON.stringify({ success: true }), { headers });
    }

    default:
      return new Response("Not found", { status: 404 });
  }
}


async function handleApiRequest(path, request, env) {
  // We've received at API request. Route the request based on the path.

  switch (path[0]) {
    case "auth":
      return handleAuthRequest(path.slice(1), request, env);

    case "rooms": {
      // Room registry endpoints.
      let registryId = env.roomRegistry.idFromName("singleton");
      let registry = env.roomRegistry.get(registryId);

      if (request.method === "GET") {
        // GET /api/rooms — list all rooms
        return registry.fetch(new Request("https://dummy/list"));
      } else if (request.method === "POST") {
        // POST /api/rooms — create a new room
        let session = await validateSession(request, env);
        if (!session) {
          return new Response(JSON.stringify({ error: "Not authenticated." }), {
            status: 401, headers: { "Content-Type": "application/json" }
          });
        }
        return registry.fetch(new Request("https://dummy/add", {
          method: "POST",
          body: request.body,
          headers: { "Content-Type": "application/json" }
        }));
      }
      return new Response("Method not allowed", { status: 405 });
    }

    case "room": {
      // Request for `/api/room/<name>/...`.
      if (!path[1]) {
        return new Response("Room name required", { status: 400 });
      }

      let name = decodeURIComponent(path[1]);
      if (name.length > 32) {
        return new Response("Name too long", { status: 404 });
      }

      let id = env.rooms.idFromName(name);

      // Get the Durable Object stub for this room! The stub is a client object that can be used
      // to send messages to the remote Durable Object instance. The stub is returned immediately;
      // there is no need to await it. This is important because you would not want to wait for
      // a network round trip before you could start sending requests. Since Durable Objects are
      // created on-demand when the ID is first used, there's nothing to wait for anyway; we know
      // an object will be available somewhere to receive our requests.
      let roomObject = env.rooms.get(id);

      // Compute a new URL with `/api/room/<name>` removed. We'll forward the rest of the path
      // to the Durable Object.
      let newUrl = new URL(request.url);
      newUrl.pathname = "/" + path.slice(2).join("/");

      // Validate session for all room requests.
      let session = await validateSession(request, env);
      if (!session) {
        return new Response("Not authenticated", { status: 401 });
      }

      // If this is a WebSocket upgrade, require display name.
      if (request.headers.get("Upgrade") === "websocket") {
        if (!session.displayName) {
          return new Response("Display name required", { status: 403 });
        }
        // Pass the authenticated user info to the ChatRoom DO via headers.
        let modifiedHeaders = new Headers(request.headers);
        modifiedHeaders.set("X-Auth-DisplayName", session.displayName);
        modifiedHeaders.set("X-Auth-Email", session.email);
        modifiedHeaders.set("X-Auth-Admin", session.admin ? "1" : "0");
        let modifiedRequest = new Request(newUrl, {
          headers: modifiedHeaders,
          method: request.method,
        });
        return roomObject.fetch(modifiedRequest);
      }

      // Export requires admin.
      if (newUrl.pathname === "/export" && !session.admin) {
        return new Response("Forbidden", { status: 403 });
      }

      // Send the request to the object.
      return roomObject.fetch(newUrl, request);
    }

    default:
      return new Response("Not found", {status: 404});
  }
}

// =======================================================================================
// The ChatRoom Durable Object Class

// ChatRoom implements a Durable Object that coordinates an individual chat room. Participants
// connect to the room using WebSockets, and the room broadcasts messages from each participant
// to all others.
export class ChatRoom {
  constructor(state, env) {
    this.state = state

    // `state.storage` provides access to our durable storage. It provides a simple KV
    // get()/put() interface.
    this.storage = state.storage;

    // `env` is our environment bindings (discussed earlier).
    this.env = env;

    // We will track metadata for each client WebSocket object in `sessions`.
    this.sessions = new Map();
    this.state.getWebSockets().forEach((webSocket) => {
      // The constructor may have been called when waking up from hibernation,
      // so get previously serialized metadata for any existing WebSockets.
      let meta = webSocket.deserializeAttachment();

      // Set up our rate limiter client.
      // The client itself can't have been in the attachment, because structured clone doesn't work on functions.
      // DO ids aren't cloneable, restore the ID from its hex string
      let limiterId = this.env.limiters.idFromString(meta.limiterId);
      let limiter = new RateLimiterClient(
        () => this.env.limiters.get(limiterId),
        err => webSocket.close(1011, err.stack));

      // We don't send any messages to the client until it has sent us the initial user info
      // message. Until then, we will queue messages in `session.blockedMessages`.
      // This could have been arbitrarily large, so we won't put it in the attachment.
      let blockedMessages = [];
      this.sessions.set(webSocket, { ...meta, limiter, blockedMessages });
    });

    // We keep track of the last-seen message's timestamp just so that we can assign monotonically
    // increasing timestamps even if multiple messages arrive simultaneously (see below). There's
    // no need to store this to disk since we assume if the object is destroyed and recreated, much
    // more than a millisecond will have gone by.
    this.lastTimestamp = 0;
  }

  // The system will call fetch() whenever an HTTP request is sent to this Object. Such requests
  // can only be sent from other Worker code, such as the code above; these requests don't come
  // directly from the internet. In the future, we will support other formats than HTTP for these
  // communications, but we started with HTTP for its familiarity.
  async fetch(request) {
    return await handleErrors(request, async () => {
      let url = new URL(request.url);

      switch (url.pathname) {
        case "/websocket": {
          // The request is to `/api/room/<name>/websocket`. A client is trying to establish a new
          // WebSocket session.
          if (request.headers.get("Upgrade") != "websocket") {
            return new Response("expected websocket", {status: 400});
          }

          // Get the client's IP address for use with the rate limiter.
          let ip = request.headers.get("CF-Connecting-IP");

          // Get the authenticated user info passed by the Worker.
          let displayName = request.headers.get("X-Auth-DisplayName");
          let email = request.headers.get("X-Auth-Email") || "";
          let admin = request.headers.get("X-Auth-Admin") === "1";

          // To accept the WebSocket request, we create a WebSocketPair (which is like a socketpair,
          // i.e. two WebSockets that talk to each other), we return one end of the pair in the
          // response, and we operate on the other end. Note that this API is not part of the
          // Fetch API standard; unfortunately, the Fetch API / Service Workers specs do not define
          // any way to act as a WebSocket server today.
          let pair = new WebSocketPair();

          // We're going to take pair[1] as our end, and return pair[0] to the client.
          await this.handleSession(pair[1], ip, displayName, email, admin);

          // Now we return the other end of the pair to the client.
          return new Response(null, { status: 101, webSocket: pair[0] });
        }

        case "/export": {
          // Export all chat history as XLSX with an Excel Table.
          let storage = await this.storage.list();
          let rows = [];
          for (let value of storage.values()) {
            let msg;
            try { msg = JSON.parse(value); } catch { continue; }
            if (!msg.message) continue;

            let ts = msg.timestamp ? new Date(msg.timestamp).toISOString() : "";
            let email = msg.email || "";
            let nickname = msg.name || "";

            let language = "";
            let english = "";
            let japanese = "";

            if (msg.translation) {
              if (msg.translation.lang === "JA") {
                language = "en";
                english = msg.message;
                japanese = msg.translation.text;
              } else if (msg.translation.lang === "EN") {
                language = "ja";
                japanese = msg.message;
                english = msg.translation.text;
              }
            } else {
              english = msg.message;
            }

            rows.push([ts, email, nickname, language, english, japanese]);
          }

          let wb = new ExcelJS.Workbook();
          let ws = wb.addWorksheet("Chat");
          ws.columns = [
            { width: 30 },
            { width: 30 },
            { width: 15 },
            { width: 8 },
            { width: 50 },
            { width: 50 },
          ];
          ws.addTable({
            name: "ChatExport",
            ref: "A1",
            headerRow: true,
            columns: [
              { name: "timestamp", filterButton: true },
              { name: "email", filterButton: true },
              { name: "nickname", filterButton: true },
              { name: "language", filterButton: true },
              { name: "english", filterButton: true },
              { name: "japanese", filterButton: true },
            ],
            rows: rows,
          });

          // Enable text wrapping on all cells
          ws.eachRow(row => {
            row.eachCell(cell => {
              cell.alignment = { wrapText: true };
            });
          });

          let buf = await wb.xlsx.writeBuffer();

          return new Response(buf, {
            headers: {
              "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
              "Content-Disposition": "attachment; filename=\"chat-export.xlsx\""
            }
          });
        }

        default:
          return new Response("Not found", {status: 404});
      }
    });
  }

  // handleSession() implements our WebSocket-based chat protocol.
  async handleSession(webSocket, ip, displayName, email, admin) {
    // Accept our end of the WebSocket. This tells the runtime that we'll be terminating the
    // WebSocket in JavaScript, not sending it elsewhere.
    this.state.acceptWebSocket(webSocket);

    // Set up our rate limiter client.
    let limiterId = this.env.limiters.idFromName(ip);
    let limiter = new RateLimiterClient(
        () => this.env.limiters.get(limiterId),
        err => webSocket.close(1011, err.stack));

    // Create our session and add it to the sessions map.
    // The display name is set server-side from the authenticated session.
    let session = { limiterId, limiter, blockedMessages: [], name: displayName, email, admin };
    // attach limiterId, name, email, and admin to the webSocket so it survives hibernation
    webSocket.serializeAttachment({ ...webSocket.deserializeAttachment(), limiterId: limiterId.toString(), name: displayName, email, admin });
    this.sessions.set(webSocket, session);

    // Queue "join" messages for all online users, to populate the client's roster.
    for (let otherSession of this.sessions.values()) {
      if (otherSession.name) {
        session.blockedMessages.push(JSON.stringify({joined: otherSession.name}));
      }
    }

    // Load the last 100 messages from the chat history stored on disk, and send them to the
    // client.
    let storage = await this.storage.list({reverse: true, limit: 100});
    let backlog = [...storage.values()];
    backlog.reverse();
    backlog.forEach(value => {
      session.blockedMessages.push(value);
    });

    // Deliver all queued messages and broadcast join.
    session.blockedMessages.forEach(queued => {
      webSocket.send(queued);
    });
    delete session.blockedMessages;

    this.broadcast({joined: session.name});
    webSocket.send(JSON.stringify({ready: true}));
  }

  async webSocketMessage(webSocket, msg) {
    try {
      let session = this.sessions.get(webSocket);
      if (session.quit) {
        webSocket.close(1011, "WebSocket broken.");
        return;
      }

      // Check if the user is over their rate limit and reject the message if so.
      if (!session.limiter.checkLimit()) {
        webSocket.send(JSON.stringify({
          error: "Your IP is being rate-limited, please try again later."
        }));
        return;
      }

      let data = JSON.parse(msg);

      // Name is set server-side at connection time, so all messages are chat messages.
      // Construct sanitized message for storage and broadcast.
      data = { name: session.name, email: session.email, admin: session.admin, message: "" + data.message };

      // Block people from sending overly long messages. This is also enforced on the client,
      // so to trigger this the user must be bypassing the client code.
      if (data.message.length > 256) {
        webSocket.send(JSON.stringify({error: "Message too long."}));
        return;
      }

      // Add timestamp. Here's where this.lastTimestamp comes in -- if we receive a bunch of
      // messages at the same time (or if the clock somehow goes backwards????), we'll assign
      // them sequential timestamps, so at least the ordering is maintained.
      data.timestamp = Math.max(Date.now(), this.lastTimestamp + 1);
      this.lastTimestamp = data.timestamp;

      // Broadcast the message to all other WebSockets.
      let dataStr = JSON.stringify(data);
      this.broadcast(dataStr);

      // Save message.
      let key = new Date(data.timestamp).toISOString();
      await this.storage.put(key, dataStr);

      // Translate asynchronously — don't block the sender
      this.translateAndBroadcast(data, key);
    } catch (err) {
      // Report any exceptions directly back to the client. As with our handleErrors() this
      // probably isn't what you'd want to do in production, but it's convenient when testing.
      webSocket.send(JSON.stringify({error: err.stack}));
    }
  }

  // On "close" and "error" events, remove the WebSocket from the sessions list and broadcast
  // a quit message.
  async closeOrErrorHandler(webSocket) {
    let session = this.sessions.get(webSocket) || {};
    session.quit = true;
    this.sessions.delete(webSocket);
    if (session.name) {
      this.broadcast({quit: session.name});
    }
  }

  async webSocketClose(webSocket, code, reason, wasClean) {
    this.closeOrErrorHandler(webSocket)
  }

  async webSocketError(webSocket, error) {
    this.closeOrErrorHandler(webSocket)
  }

  // translateAndBroadcast() translates a message and broadcasts the translation to all clients.
  async translateAndBroadcast(data, key) {
    try {
      // First try translating to JA; DeepL will tell us the detected source language.
      let result = await translateText(data.message, "JA", this.env.DEEPL_API_KEY);
      if (!result) return;

      let targetLang = "JA";
      // If the source was already Japanese, translate to English instead.
      if (result.detectedSourceLang === "JA") {
        result = await translateText(data.message, "EN", this.env.DEEPL_API_KEY);
        if (!result) return;
        targetLang = "EN";
      }

      let translation = { text: result.translatedText, lang: targetLang };

      // Update stored message with translation
      let updated = { ...data, translation };
      await this.storage.put(key, JSON.stringify(updated));

      // Broadcast translation update to all connected clients
      this.broadcast({ type: "translation", timestamp: data.timestamp, text: translation.text, lang: translation.lang });
    } catch {
      // Translation failure is non-fatal
    }
  }

  // broadcast() broadcasts a message to all clients.
  broadcast(message) {
    // Apply JSON if we weren't given a string to start with.
    if (typeof message !== "string") {
      message = JSON.stringify(message);
    }

    // Iterate over all the sessions sending them messages.
    let quitters = [];
    this.sessions.forEach((session, webSocket) => {
      if (session.name) {
        try {
          webSocket.send(message);
        } catch (err) {
          // Whoops, this connection is dead. Remove it from the map and arrange to notify
          // everyone below.
          session.quit = true;
          quitters.push(session);
          this.sessions.delete(webSocket);
        }
      } else {
        // This session hasn't sent the initial user info message yet, so we're not sending them
        // messages yet (no secret lurking!). Queue the message to be sent later.
        session.blockedMessages.push(message);
      }
    });

    quitters.forEach(quitter => {
      if (quitter.name) {
        this.broadcast({quit: quitter.name});
      }
    });
  }
}

// =======================================================================================
// The RateLimiter Durable Object class.

// RateLimiter implements a Durable Object that tracks the frequency of messages from a particular
// source and decides when messages should be dropped because the source is sending too many
// messages.
//
// We utilize this in ChatRoom, above, to apply a per-IP-address rate limit. These limits are
// global, i.e. they apply across all chat rooms, so if a user spams one chat room, they will find
// themselves rate limited in all other chat rooms simultaneously.
export class RateLimiter {
  constructor(state, env) {
    // Timestamp at which this IP will next be allowed to send a message. Start in the distant
    // past, i.e. the IP can send a message now.
    this.nextAllowedTime = 0;
  }

  // Our protocol is: POST when the IP performs an action, or GET to simply read the current limit.
  // Either way, the result is the number of seconds to wait before allowing the IP to perform its
  // next action.
  async fetch(request) {
    return await handleErrors(request, async () => {
      let now = Date.now() / 1000;

      this.nextAllowedTime = Math.max(now, this.nextAllowedTime);

      if (request.method == "POST") {
        // POST request means the user performed an action.
        // We allow one action per 5 seconds.
        this.nextAllowedTime += 5;
      }

      // Return the number of seconds that the client needs to wait.
      //
      // We provide a "grace" period of 20 seconds, meaning that the client can make 4-5 requests
      // in a quick burst before they start being limited.
      let cooldown = Math.max(0, this.nextAllowedTime - now - 20);
      return new Response(cooldown);
    })
  }
}

// RateLimiterClient implements rate limiting logic on the caller's side.
class RateLimiterClient {
  // The constructor takes two functions:
  // * getLimiterStub() returns a new Durable Object stub for the RateLimiter object that manages
  //   the limit. This may be called multiple times as needed to reconnect, if the connection is
  //   lost.
  // * reportError(err) is called when something goes wrong and the rate limiter is broken. It
  //   should probably disconnect the client, so that they can reconnect and start over.
  constructor(getLimiterStub, reportError) {
    this.getLimiterStub = getLimiterStub;
    this.reportError = reportError;

    // Call the callback to get the initial stub.
    this.limiter = getLimiterStub();

    // When `inCooldown` is true, the rate limit is currently applied and checkLimit() will return
    // false.
    this.inCooldown = false;
  }

  // Call checkLimit() when a message is received to decide if it should be blocked due to the
  // rate limit. Returns `true` if the message should be accepted, `false` to reject.
  checkLimit() {
    if (this.inCooldown) {
      return false;
    }
    this.inCooldown = true;
    this.callLimiter();
    return true;
  }

  // callLimiter() is an internal method which talks to the rate limiter.
  async callLimiter() {
    try {
      let response;
      try {
        // Currently, fetch() needs a valid URL even though it's not actually going to the
        // internet. We may loosen this in the future to accept an arbitrary string. But for now,
        // we have to provide a dummy URL that will be ignored at the other end anyway.
        response = await this.limiter.fetch("https://dummy-url", {method: "POST"});
      } catch (err) {
        // `fetch()` threw an exception. This is probably because the limiter has been
        // disconnected. Stubs implement E-order semantics, meaning that calls to the same stub
        // are delivered to the remote object in order, until the stub becomes disconnected, after
        // which point all further calls fail. This guarantee makes a lot of complex interaction
        // patterns easier, but it means we must be prepared for the occasional disconnect, as
        // networks are inherently unreliable.
        //
        // Anyway, get a new limiter and try again. If it fails again, something else is probably
        // wrong.
        this.limiter = this.getLimiterStub();
        response = await this.limiter.fetch("https://dummy-url", {method: "POST"});
      }

      // The response indicates how long we want to pause before accepting more requests.
      let cooldown = +(await response.text());
      await new Promise(resolve => setTimeout(resolve, cooldown * 1000));

      // Done waiting.
      this.inCooldown = false;
    } catch (err) {
      this.reportError(err);
    }
  }
}

// =======================================================================================
// The RoomRegistry Durable Object class.
//
// A singleton that maintains the list of public room names.

export class RoomRegistry {
  constructor(state, env) {
    this.state = state;
    this.storage = state.storage;
  }

  async fetch(request) {
    return await handleErrors(request, async () => {
      let url = new URL(request.url);

      switch (url.pathname) {
        case "/list": {
          let rooms = (await this.storage.get("rooms")) || [];
          return new Response(JSON.stringify({ rooms }), {
            headers: { "Content-Type": "application/json" }
          });
        }

        case "/add": {
          let { name } = await request.json();
          if (!name || typeof name !== "string") {
            return new Response(JSON.stringify({ error: "Room name is required." }), {
              status: 400, headers: { "Content-Type": "application/json" }
            });
          }

          name = name.trim();
          if (name.length === 0 || name.length > 32) {
            return new Response(JSON.stringify({ error: "Room name must be 1-32 characters." }), {
              status: 400, headers: { "Content-Type": "application/json" }
            });
          }

          let rooms = (await this.storage.get("rooms")) || [];
          if (rooms.includes(name)) {
            return new Response(JSON.stringify({ error: "Room already exists." }), {
              status: 409, headers: { "Content-Type": "application/json" }
            });
          }

          rooms.push(name);
          await this.storage.put("rooms", rooms);

          return new Response(JSON.stringify({ success: true, name }), {
            headers: { "Content-Type": "application/json" }
          });
        }

        default:
          return new Response("Not found", { status: 404 });
      }
    });
  }
}

// =======================================================================================
// The AuthSession Durable Object class.
//
// AuthSession manages session tokens for a single email address.
// It is keyed by normalized email via idFromName(email).

const MAX_SESSIONS = 10;
const SESSION_MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000; // 30 days

export class AuthSession {
  constructor(state, env) {
    this.state = state;
    this.storage = state.storage;
    this.env = env;
  }

  async fetch(request) {
    return await handleErrors(request, async () => {
      let url = new URL(request.url);

      switch (url.pathname) {
        case "/create-session": {
          let now = Date.now();

          // Generate session token
          let tokenBytes = new Uint8Array(32);
          crypto.getRandomValues(tokenBytes);
          let token = [...tokenBytes].map(b => b.toString(16).padStart(2, "0")).join("");

          // Load existing sessions, prune expired, add new, cap at MAX_SESSIONS
          let sessions = (await this.storage.get("sessions")) || [];
          sessions = sessions.filter(s => s.expiresAt > now);
          if (sessions.length >= MAX_SESSIONS) {
            // Evict oldest
            sessions.sort((a, b) => a.expiresAt - b.expiresAt);
            sessions = sessions.slice(sessions.length - MAX_SESSIONS + 1);
          }
          sessions.push({ token, expiresAt: now + SESSION_MAX_AGE_MS });
          await this.storage.put("sessions", sessions);

          return new Response(JSON.stringify({ token }), {
            headers: { "Content-Type": "application/json" }
          });
        }

        case "/validate": {
          let { token } = await request.json();
          let now = Date.now();

          let sessions = (await this.storage.get("sessions")) || [];
          let session = sessions.find(s => s.token === token && s.expiresAt > now);
          if (!session) {
            return new Response(JSON.stringify({ error: "Invalid session." }), {
              status: 401, headers: { "Content-Type": "application/json" }
            });
          }

          let displayName = (await this.storage.get("displayName")) || null;
          return new Response(JSON.stringify({ valid: true, displayName }), {
            headers: { "Content-Type": "application/json" }
          });
        }

        case "/set-name": {
          let { displayName } = await request.json();
          await this.storage.put("displayName", displayName);
          return new Response(JSON.stringify({ success: true }), {
            headers: { "Content-Type": "application/json" }
          });
        }

        case "/remove-session": {
          let { token } = await request.json();
          let sessions = (await this.storage.get("sessions")) || [];
          sessions = sessions.filter(s => s.token !== token);
          await this.storage.put("sessions", sessions);
          return new Response(JSON.stringify({ success: true }), {
            headers: { "Content-Type": "application/json" }
          });
        }

        default:
          return new Response("Not found", { status: 404 });
      }
    });
  }
}
