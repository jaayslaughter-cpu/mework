const express = require('express');
const app = express();

app.get('/health', (req, res) => res.json({ status: 'ok' }));

app.listen(3002, () => console.log('PropIQ Hub listening on :3002'));
