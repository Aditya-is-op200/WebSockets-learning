import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';

import { WebSocketServer } from 'ws';
import { redisPublish, redisSubscribe } from './connection.js';

const PORT = process.env.PORT ?? 9000;
const REDIS_CHANNEL = 'ws-messages';

const httpServer = http.createServer(async function (req, res) {
    const indexFile = await fs.readFile(path.resolve('./index.html'), 'utf-8');
    res.setHeader('Content-Type', 'text/html');
    return res.end(indexFile);
});

const wsServer = new WebSocketServer({ server: httpServer });

redisSubscribe.subscribe(REDIS_CHANNEL);
redisSubscribe.on('message', (channel, message) => {
    if (channel === REDIS_CHANNEL) {
        // Broadcast message to all of your connected client
        wsServer.clients.forEach((client) => {
            client.send(message.toString());
        });
    }
});

wsServer.on('connection', (websocket) => {
    console.log(`Websocket Connnection....`);

    websocket.on('message', async (data) => {
        console.log(`WebSocket Message Recv.`, data.toString());

        // RELAY THE MESSAGE TO THE BROKER...
        console.log(`Relaying Message to Redis Broker...`);
        await redisPublish.publish(REDIS_CHANNEL, data.toString());
    });
});

httpServer.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});