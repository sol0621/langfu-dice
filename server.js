/**
 * 郎府摇骰子 v2.0 - 完全对齐 PRD V5.4
 * Node.js + ws, 纯内存状态管理
 */

const http = require('http');
const { WebSocketServer } = require('ws');
const fs = require('fs');
const path = require('path');
const url = require('url');

// ==================== 常量 ====================
const PORT = process.env.PORT || 3000;
const DICE_POKER = 'dice_poker';
const GUESS_SIZE = 'guess_size';
const TWENTY_ONE = 'twenty_one';
const GAME_MODES = [DICE_POKER, GUESS_SIZE, TWENTY_ONE];

// ==================== 工具函数 ====================
function rollDice(n) { return Array.from({ length: n }, () => Math.floor(Math.random() * 6) + 1); }
function sum(arr) { return arr.reduce((a, b) => a + b, 0); }

function genRoomId() {
  for (let i = 0; i < 100; i++) {
    const id = String(Math.floor(Math.random() * 9000) + 1000).padStart(4, '0');
    if (!rooms.has(id)) return id;
  }
  return null;
}

function genUserId() { return 'u_' + Date.now().toString(36) + Math.random().toString(36).substr(2, 5); }

// ==================== 存储 ====================
const rooms = new Map();
const connMap = new Map(); // userId -> { ws, userId, roomId, nickname }

function getRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room) throw new Error('ROOM_NOT_FOUND');
  return room;
}

function getPlayer(roomId, userId) {
  const room = getRoom(roomId);
  const player = room.players.find(p => p.userId === userId);
  if (!player) throw new Error('PLAYER_NOT_FOUND');
  return { room, player };
}

// ==================== 大话骰叫骰阶段：检查叫骰合法性 ====================
let currentCall = null; // { count, point, userId, round }

function resetCurrentCall() {
  currentCall = null;
}

function validateCall(count, point) {
  if (point < 1 || point > 6) return { ok: false, msg: '点数需在 1-6 之间' };
  if (!currentCall) {
    if (count < 1) return { ok: false, msg: '个数至少为 1' };
    return { ok: true, msg: 'call_added' };
  }
  // 必须大于当前叫骰：个数更大，或个数相同但点数更大
  if (count > currentCall.count) return { ok: true, msg: 'call_added' };
  if (count === currentCall.count && point > currentCall.point) return { ok: true, msg: 'call_added' };
  return { ok: false, msg: `需大于 ${currentCall.count}个${currentCall.point}` };
}

// ==================== 广播 ====================
function send(ws, msg) { if (ws.readyState === 1) ws.send(JSON.stringify(msg)); }

function broadcast(roomId, msg) {
  const room = rooms.get(roomId);
  if (!room) return;
  room.players.forEach(p => {
    const c = connMap.get(p.userId);
    if (c && c.ws && c.ws.readyState === 1) send(c.ws, msg);
  });
}

function sendTo(userId, msg) {
  const c = connMap.get(userId);
  if (c && c.ws && c.ws.readyState === 1) send(c.ws, msg);
}

// ==================== room_update 构建（不含私人骰子） ====================
function buildRoomUpdate(room) {
  return {
    type: 'room_update',
    data: {
      roomId: room.roomId,
      roomStatus: room.roomStatus,
      gameMode: room.gameMode,
      hostId: room.hostId,
      currentRound: room.currentRound,
      winnerId: room.winnerId,
      players: room.players.map(p => ({
        userId: p.userId,
        nickname: p.nickname,
        isAlive: p.isAlive,
        isStopped: p.isStopped || false,
        isBusted: p.isBusted || false,
        isRolled: p.isRolled || false,
        disconnected: p.disconnected || false
      })),
      // 猜大小：公共骰子是公开数据
      publicDiceValues: room.publicDiceValues,
      publicTotal: room.publicTotal,
      publicResult: room.publicResult,
      callOrder: [...room.callOrder],
      currentCallerId: getCurrentCallerId(room)
    }
  };
}

// ==================== HTTP 服务器 ====================
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.writeHead(200); res.end(); return; }

  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;

  // 静态文件
  if (req.method === 'GET' && !pathname.startsWith('/room/')) {
    let filePath = pathname === '/' ? '/index.html' : pathname;
    filePath = path.join(__dirname, 'public', filePath);
    const extMap = { '.html': 'text/html; charset=utf-8', '.css': 'text/css', '.js': 'application/javascript', '.json': 'application/json', '.png': 'image/png' };
    const contentType = extMap[path.extname(filePath)] || 'text/plain';
    fs.readFile(filePath, (err, data) => {
      if (err) { res.writeHead(404); res.end('Not Found'); return; }
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(data);
    });
    return;
  }

  // POST 路由
  if (req.method === 'POST') {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const data = body ? JSON.parse(body) : {};
        if (pathname === '/room/create') handleHttpCreateRoom(data, res);
        else if (pathname === '/room/join') handleHttpJoinRoom(data, res);
        else { res.writeHead(404); res.end(JSON.stringify({ error: 'NOT_FOUND' })); }
      } catch (e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: e.message || 'BAD_REQUEST' }));
      }
    });
    return;
  }

  res.writeHead(404); res.end('Not Found');
});

function jsonRes(res, code, data) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(data));
}

// ==================== HTTP: 创建房间 ====================
function handleHttpCreateRoom(data, res) {
  const nickname = (data.nickname || '').trim();
  if (!nickname || nickname.length > 8) {
    return jsonRes(res, 400, { error: '昵称需1-8个字符' });
  }
  const mode = GAME_MODES.includes(data.gameMode) ? data.gameMode : DICE_POKER;

  // 房间号：优先用客户端指定的（4位数字），否则服务器生成
  let roomId = (data.roomId || '').trim();
  if (!/^\d{4}$/.test(roomId)) {
    roomId = genRoomId();
  } else if (rooms.has(roomId)) {
    return jsonRes(res, 400, { error: '房间号已存在，请换一个' });
  }
  if (!roomId) return jsonRes(res, 500, { error: '房间创建失败，请重试' });

  // userId：优先用客户端指定的（保持身份一致），否则生成
  const userId = (data.userId || '').trim() || genUserId();
  const diceCount = parseInt(data.diceCount, 10);
  const validDiceCount = (diceCount >= 3 && diceCount <= 6) ? diceCount : 5;
  const room = {
    roomId,
    roomStatus: 'idle',
    gameMode: mode,
    diceCount: validDiceCount,
    hostId: userId,
    currentRound: 0,
    winnerId: null,
    players: [{
      userId,
      nickname,
      isAlive: true,
      isStopped: false,
      isBusted: false,
      isRolled: false,
      diceValues: [],
      total: 0,
      disconnected: false
    }],
    publicDiceValues: null,
    publicTotal: null,
    publicResult: null,
    callOrder: [userId],        // 叫骰轮流顺序（加入顺序）
    currentCallerIndex: 0,
    createdAt: Date.now()
  };

  rooms.set(roomId, room);
  jsonRes(res, 200, { roomId, userId });
}

// ==================== HTTP: 加入房间 ====================
function handleHttpJoinRoom(data, res) {
  const nickname = (data.nickname || '').trim();
  const roomId = (data.roomId || '').trim();
  if (!nickname || nickname.length > 8) return jsonRes(res, 400, { error: '昵称需1-8个字符' });
  if (!/^\d{4}$/.test(roomId)) return jsonRes(res, 400, { error: '房间号需4位数字' });

  const room = rooms.get(roomId);
  if (!room) return jsonRes(res, 404, { error: '房间不存在' });
  if (room.roomStatus !== 'idle') return jsonRes(res, 400, { error: '游戏进行中，请等待本局结束' });
  if (room.players.length >= 20) return jsonRes(res, 400, { error: '房间已满（最多20人）' });
  if (room.players.some(p => p.nickname === nickname)) return jsonRes(res, 400, { error: '昵称已被使用' });

  const userId = (data.userId || '').trim() || genUserId();
  room.players.push({
    userId, nickname,
    isAlive: true, isStopped: false, isBusted: false, isRolled: false,
    diceValues: [], total: 0, disconnected: false
  });
  room.callOrder.push(userId);

  // 广播更新给已在线的玩家
  broadcast(roomId, buildRoomUpdate(room));
  jsonRes(res, 200, { roomId, userId });
}

// ==================== WebSocket ====================
const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  // 从 query 参数获取 userId 和 roomId
  const params = url.parse(req.url, true).query;
  const userId = params.userId;
  const roomId = params.roomId;

  if (!userId || !roomId) {
    ws.close(4001, '缺少 userId 或 roomId');
    return;
  }

  const room = rooms.get(roomId);
  if (!room) { ws.close(4002, '房间不存在'); return; }

  const player = room.players.find(p => p.userId === userId);
  if (!player) { ws.close(4003, '不在房间中'); return; }

  // 注册连接
  const existing = connMap.get(userId);
  if (existing && existing.ws && existing.ws.readyState === 1) {
    existing.ws.close(4000, '新连接替换');
  }
  connMap.set(userId, { ws, userId, roomId, nickname: player.nickname });
  player.disconnected = false;

  ws.userId = userId;
  ws.roomId = roomId;
  ws.nickname = player.nickname;

  // 发送 userId 确认
  send(ws, { type: 'user_id', data: { userId } });

  // 入座：广播 room_update
  broadcast(roomId, buildRoomUpdate(room));

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      handleMessage(ws, msg);
    } catch (e) {
      send(ws, { type: 'error', data: { message: e.message || '消息格式错误' } });
    }
  });

  ws.on('close', () => handleDisconnect(ws));
  ws.on('error', () => {});
});

function handleDisconnect(ws) {
  const userId = ws.userId;
  const roomId = ws.roomId;
  if (!roomId) return;

  const room = rooms.get(roomId);
  if (!room) return;

  const player = room.players.find(p => p.userId === userId);
  if (!player) return;

  player.disconnected = true;
  broadcast(roomId, buildRoomUpdate(room));

  // 5分钟后仍未重连，清理
  setTimeout(() => {
    const p = room.players.find(pl => pl.userId === userId);
    if (p && p.disconnected) {
      removePlayerFromRoom(room, userId);
      broadcast(room.roomId, buildRoomUpdate(room));
      if (room.players.length === 0) rooms.delete(room.roomId);
    }
  }, 5 * 60 * 1000);
}

function removePlayerFromRoom(room, userId) {
  const idx = room.players.findIndex(p => p.userId === userId);
  if (idx !== -1) room.players.splice(idx, 1);
  connMap.delete(userId);
  // 从叫骰顺序中移除
  const orderIdx = room.callOrder.indexOf(userId);
  if (orderIdx !== -1) {
    room.callOrder.splice(orderIdx, 1);
    // 如果移除的是当前叫骰者之前的人，index需要调整
    if (orderIdx <= room.currentCallerIndex && room.currentCallerIndex > 0) {
      room.currentCallerIndex--;
    }
    // 如果移除后列表空了，重置
    if (room.callOrder.length === 0) room.currentCallerIndex = 0;
    else room.currentCallerIndex = room.currentCallerIndex % room.callOrder.length;
  }
  // 如果房主离开，转移给第一个玩家
  if (room.hostId === userId && room.players.length > 0) {
    room.hostId = room.players[0].userId;
  }
}

// ==================== 叫骰轮流制 ====================
function getCurrentCallerId(room) {
  if (!room.callOrder || room.callOrder.length === 0) return null;
  return room.callOrder[room.currentCallerIndex % room.callOrder.length];
}

function advanceTurn(room) {
  if (!room.callOrder || room.callOrder.length === 0) return;
  room.currentCallerIndex = (room.currentCallerIndex + 1) % room.callOrder.length;
  broadcastTurn(room);
}

function resetTurn(room) {
  room.currentCallerIndex = 0;
  broadcastTurn(room);
}

function broadcastTurn(room) {
  broadcast(room.roomId, {
    type: 'turn_update',
    data: {
      currentCallerId: getCurrentCallerId(room),
      callOrder: [...room.callOrder]
    }
  });
}

// ==================== 消息路由 ====================
function handleMessage(ws, msg) {
  const { type, data } = msg || {};
  try {
    switch (type) {
      case 'start_game':    handleStartGame(ws); break;
      case 'switch_mode':   handleSwitchMode(ws, data); break;
      case 'challenge':     handleChallenge(ws, data); break;
      case 'eliminate_self': handleEliminateSelf(ws); break;
      case 'hit':           handleHit(ws); break;
      case 'stand':         handleStand(ws); break;
      case 'next_round':    handleNextRound(ws); break;
      case 'roll_dice':     handleRollDice(ws); break;
      case 'call_dice':     handleCallDice(ws, data); break;
      case 'challenge_player': handleChallengePlayer(ws, data); break;
      case 'reset_round':   handleResetRound(ws); break;
      default: send(ws, { type: 'error', data: { message: `未知消息类型: ${type}` } });
    }
  } catch (e) {
    send(ws, { type: 'error', data: { message: e.message } });
  }
}

// ==================== 摇骰子（在线摇模式） ====================
function handleRollDice(ws) {
  const { room, player } = getRoomPlayer(ws);
  if (room.roomStatus !== 'idle' && room.roomStatus !== 'rolling') {
    throw new Error('NOT_IDLE');
  }

  // 设置为 rolling 状态
  if (room.roomStatus === 'idle') {
    room.roomStatus = 'rolling';
    resetCurrentCall();
    broadcast(room.roomId, { type: 'call_reset', data: {} });
  }

  // 使用房间配置的骰子数量
  const diceCount = room.diceCount || 5;
  player.diceValues = rollDice(diceCount);
  player.total = sum(player.diceValues);
  player.isRolled = true;

  // 发送私人骰子结果
  sendTo(player.userId, {
    type: 'dice_result',
    data: { diceValues: [...player.diceValues], total: player.total }
  });

  // 广播玩家已摇状态
  broadcast(room.roomId, buildRoomUpdate(room));

  // 通知该玩家自己的骰子结果
  send(ws, { type: 'roll_ack', data: { diceCount, rolled: true } });
}

// ==================== 叫骰 ====================
function handleCallDice(ws, data) {
  const { room, player } = getRoomPlayer(ws);
  if (room.gameMode !== DICE_POKER) throw new Error('WRONG_MODE');
  if (!player.isRolled) throw new Error('NOT_ROLLED');

  const count = parseInt(data.count, 10);
  const point = parseInt(data.point, 10);

  const validation = validateCall(count, point);
  if (!validation.ok) throw new Error(validation.msg);

  currentCall = { count, point, userId: player.userId, round: room.currentRound || 1 };

  // 广播叫骰
  broadcast(room.roomId, {
    type: 'call_dice_broadcast',
    data: {
      userId: player.userId,
      nickname: player.nickname,
      count,
      point
    }
  });

  // 叫骰后轮转到下一个人
  advanceTurn(room);
}

// ==================== 开骰（指定玩家） ====================
function handleChallengePlayer(ws, data) {
  const { room, player } = getRoomPlayer(ws);
  if (room.gameMode !== DICE_POKER) throw new Error('WRONG_MODE');
  if (!player.isRolled) throw new Error('NOT_ROLLED');

  const targetId = data.targetUserId;
  const target = room.players.find(p => p.userId === targetId);
  if (!target) throw new Error('PLAYER_NOT_FOUND');
  if (!target.isRolled) throw new Error('TARGET_NOT_ROLLED');

  // 计算百搭（1点万能牌）
  const wildcardCount = target.diceValues.filter(v => v === 1).length;
  // 各点数实际计数（含百搭，1点自身不算百搭加成）
  const pointCounts = {};
  for (let p = 1; p <= 6; p++) {
    pointCounts[p] = target.diceValues.filter(v => v === p).length + (p === 1 ? 0 : wildcardCount);
  }

  // 校验：被开的人是不是当前叫骰者，是的话验证叫骰是否成立
  let callValid = null, calledCount = null, calledPoint = null, actualCount = null;
  if (currentCall && currentCall.userId === targetId) {
    calledCount = currentCall.count;
    calledPoint = currentCall.point;
    // 1点为百搭，可算作任意点数
    actualCount = target.diceValues.filter(v => v === calledPoint || v === 1).length;
    callValid = actualCount >= calledCount;
  }

  // 广播被开玩家的真实骰子 + 百搭信息 + 叫骰验证
  broadcast(room.roomId, {
    type: 'challenge_player_result',
    data: {
      challengerId: player.userId,
      challengerNickname: player.nickname,
      targetId: target.userId,
      targetNickname: target.nickname,
      diceValues: [...target.diceValues],
      wildcardCount,
      callValid,
      calledCount,
      calledPoint,
      actualCount
    }
  });
}

// ==================== 重置本局 ====================
function handleResetRound(ws) {
  const { room } = getRoomPlayer(ws);
  if (room.hostId !== ws.userId) throw new Error('NOT_HOST');

  room.roomStatus = 'idle';
  resetCurrentCall();
  resetTurn(room); // 重置叫骰轮流
  room.winnerId = null;
  room.publicDiceValues = null;
  room.publicTotal = null;
  room.publicResult = null;
  broadcast(room.roomId, { type: 'call_reset', data: {} });

  room.players.forEach(p => {
    p.isAlive = true;
    p.isStopped = false;
    p.isBusted = false;
    p.isRolled = false;
    p.diceValues = [];
    p.total = 0;
  });

  broadcast(room.roomId, buildRoomUpdate(room));
}

// ==================== 开始游戏 ====================
function handleStartGame(ws) {
  const { room } = getRoomPlayer(ws);
  if (room.hostId !== ws.userId) throw new Error('NOT_HOST');
  if (room.players.length < 2) throw new Error('NOT_ENOUGH_PLAYERS');
  if (room.roomStatus !== 'idle') throw new Error('INVALID_STATUS');

  room.roomStatus = 'playing';
  room.currentRound++;
  room.winnerId = null;
  resetCurrentCall();
  resetTurn(room); // 初始化叫骰轮流顺序
  broadcast(room.roomId, { type: 'call_reset', data: {} });
  room.publicDiceValues = null;
  room.publicTotal = null;
  room.publicResult = null;

  // 重置所有玩家
  room.players.forEach(p => {
    p.isAlive = true;
    p.isStopped = false;
    p.isBusted = false;
    p.diceValues = [];
    p.total = 0;
  });

  broadcast(room.roomId, buildRoomUpdate(room));

  // 2秒摇骰动画
  setTimeout(() => {
    if (!rooms.has(room.roomId)) return;

    if (room.gameMode === DICE_POKER) {
      // 大话骰：每人配置数量，私密
      const dc = room.diceCount || 5;
      room.players.forEach(p => {
        if (p.isAlive) p.diceValues = rollDice(dc);
      });
      room.players.forEach(p => {
        if (p.isAlive) sendTo(p.userId, { type: 'dice_result', data: { diceValues: p.diceValues } });
      });
    } else if (room.gameMode === GUESS_SIZE) {
      // 猜大小：3颗公共骰子
      room.publicDiceValues = rollDice(3);
      room.publicTotal = sum(room.publicDiceValues);
      const allSame = room.publicDiceValues[0] === room.publicDiceValues[1] && room.publicDiceValues[1] === room.publicDiceValues[2];
      room.publicResult = allSame ? 'triple' : (room.publicTotal <= 10 ? 'small' : 'big');
    } else if (room.gameMode === TWENTY_ONE) {
      // 21点：每人2颗
      room.players.forEach(p => {
        if (p.isAlive) {
          p.diceValues = rollDice(2);
          p.total = sum(p.diceValues);
          p.isStopped = false;
          p.isBusted = p.total > 21;
          if (p.isBusted) p.isAlive = false;
        }
      });
      room.players.forEach(p => {
        if (p.isAlive) {
          sendTo(p.userId, {
            type: 'dice_result',
            data: { diceValues: p.diceValues, total: p.total }
          });
        }
      });
    }

    broadcast(room.roomId, buildRoomUpdate(room));

    // 猜大小：自动开骰
    if (room.gameMode === GUESS_SIZE) {
      broadcast(room.roomId, {
        type: 'guess_result',
        data: {
          diceValues: room.publicDiceValues,
          total: room.publicTotal,
          result: room.publicResult
        }
      });
    }

    // 21点：检查初始状态
    if (room.gameMode === TWENTY_ONE) {
      checkTwentyOneState(room);
    }
  }, 2000);
}

// ==================== 大话骰：开骰 ====================
function handleChallenge(ws, data) {
  const { room, player } = getRoomPlayer(ws);
  if (room.gameMode !== DICE_POKER) throw new Error('WRONG_MODE');
  if (room.roomStatus !== 'playing') throw new Error('NOT_PLAYING');
  if (!player.isAlive) throw new Error('NOT_ALIVE');

  const targetPoint = data.targetPoint;
  if (targetPoint < 1 || targetPoint > 6) throw new Error('INVALID_POINT');

  const alivePlayers = room.players.filter(p => p.isAlive);

  // 统计全场该点数的出现次数（1是万能点）
  let actualCount = 0;
  alivePlayers.forEach(p => {
    p.diceValues.forEach(v => {
      if (v === targetPoint || v === 1) actualCount++;
    });
  });

  // 广播开骰结果（包含所有人骰子）
  broadcast(room.roomId, {
    type: 'challenge_result',
    data: {
      targetPoint,
      actualCount,
      allDice: alivePlayers.map(p => ({
        userId: p.userId,
        nickname: p.nickname,
        diceValues: [...p.diceValues]
      }))
    }
  });
}

// ==================== 手动认输 ====================
function handleEliminateSelf(ws) {
  const { room, player } = getRoomPlayer(ws);
  if (!player.isAlive) throw new Error('NOT_ALIVE');

  player.isAlive = false;

  broadcast(room.roomId, {
    type: 'eliminate_broadcast',
    data: { userId: player.userId, nickname: player.nickname }
  });

  const aliveCount = room.players.filter(p => p.isAlive).length;
  if (aliveCount <= 1) {
    const winner = room.players.find(p => p.isAlive);
    endGame(room, winner ? winner.userId : null);
  } else {
    broadcast(room.roomId, buildRoomUpdate(room));
  }
}

// ==================== 21点：加骰 ====================
function handleHit(ws) {
  const { room, player } = getRoomPlayer(ws);
  if (room.gameMode !== TWENTY_ONE) throw new Error('WRONG_MODE');
  if (room.roomStatus !== 'playing') throw new Error('NOT_PLAYING');
  if (!player.isAlive || player.isStopped || player.isBusted) throw new Error('CANT_HIT');
  if (player.diceValues.length >= 5) throw new Error('MAX_DICE');

  const newDie = rollDice(1)[0];
  player.diceValues.push(newDie);
  player.total = sum(player.diceValues);

  if (player.total > 21) {
    player.isBusted = true;
    player.isAlive = false;
  } else if (player.total === 21) {
    player.isStopped = true; // 自动停骰
  }

  sendTo(player.userId, {
    type: 'hit_result',
    data: { diceValues: [...player.diceValues], total: player.total, isBusted: player.isBusted }
  });

  broadcast(room.roomId, buildRoomUpdate(room));
  checkTwentyOneState(room);
}

// ==================== 21点：停骰 ====================
function handleStand(ws) {
  const { room, player } = getRoomPlayer(ws);
  if (room.gameMode !== TWENTY_ONE) throw new Error('WRONG_MODE');
  if (room.roomStatus !== 'playing') throw new Error('NOT_PLAYING');
  if (!player.isAlive || player.isBusted) throw new Error('CANT_STAND');
  if (player.isStopped) throw new Error('ALREADY_STOPPED');

  player.isStopped = true;
  broadcast(room.roomId, buildRoomUpdate(room));
  checkTwentyOneState(room);
}

function checkTwentyOneState(room) {
  // 还在游戏中（未停骰未爆掉）的玩家
  const activePlayers = room.players.filter(p => p.isAlive && !p.isStopped && !p.isBusted);
  if (activePlayers.length === 0) {
    resolveTwentyOne(room);
  } else if (activePlayers.length === 1) {
    // 只剩1人还在操作，其他人已停/爆
    const notBusted = room.players.filter(p => p.isAlive && !p.isBusted);
    if (notBusted.length === 1) {
      // 只有1个存活→自动获胜
      endGame(room, notBusted[0].userId);
    }
  }
}

function resolveTwentyOne(room) {
  const notBusted = room.players.filter(p => !p.isBusted && p.isAlive);

  if (notBusted.length === 0) {
    endGame(room, null);
    return;
  }

  const maxTotal = Math.max(...notBusted.map(p => p.total));
  const winners = notBusted.filter(p => p.total === maxTotal);

  // 亮牌
  const allPlayers = room.players.filter(p => p.diceValues.length > 0);
  broadcast(room.roomId, {
    type: 'twenty_one_reveal',
    data: {
      players: allPlayers.map(p => ({
        userId: p.userId,
        nickname: p.nickname,
        diceValues: p.diceValues,
        total: p.total,
        isBusted: p.isBusted
      })),
      winners: winners.map(w => w.userId)
    }
  });

  if (winners.length === 1) {
    endGame(room, winners[0].userId);
  } else if (winners.length > 1) {
    endGame(room, null); // 并列
  }
}

// ==================== 游戏结束 ====================
function endGame(room, winnerId) {
  room.roomStatus = 'gameover';
  room.winnerId = winnerId;

  broadcast(room.roomId, {
    type: 'game_over',
    data: { winnerId }
  });
  broadcast(room.roomId, buildRoomUpdate(room));
}

// ==================== 下一局 ====================
function handleNextRound(ws) {
  const { room } = getRoomPlayer(ws);
  if (room.roomStatus !== 'gameover') throw new Error('NOT_GAMEOVER');

  room.roomStatus = 'idle';
  room.winnerId = null;

  // 复活所有玩家
  room.players.forEach(p => {
    p.isAlive = true;
    p.isStopped = false;
    p.isBusted = false;
    p.diceValues = [];
    p.total = 0;
  });

  broadcast(room.roomId, buildRoomUpdate(room));
}

// ==================== 模式切换 ====================
function handleSwitchMode(ws, data) {
  const { room } = getRoomPlayer(ws);
  if (room.hostId !== ws.userId) throw new Error('NOT_HOST');
  if (room.roomStatus !== 'idle') throw new Error('GAME_IN_PROGRESS');

  const mode = data.gameMode;
  if (!GAME_MODES.includes(mode)) throw new Error('INVALID_MODE');

  room.gameMode = mode;
  room.publicDiceValues = null;
  room.publicTotal = null;
  room.publicResult = null;
  room.currentRound = 0;
  room.winnerId = null;

  // 重置所有玩家状态
  room.players.forEach(p => {
    p.isAlive = true;
    p.isStopped = false;
    p.isBusted = false;
    p.diceValues = [];
    p.total = 0;
  });

  broadcast(room.roomId, buildRoomUpdate(room));
}

// ==================== 辅助 ====================
function getRoomPlayer(ws) {
  const room = getRoom(ws.roomId);
  const player = room.players.find(p => p.userId === ws.userId);
  if (!player) throw new Error('PLAYER_NOT_FOUND');
  return { room, player };
}

// ==================== 启动 ====================
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🎲 郎府摇骰子 v2.0 已启动 -> http://0.0.0.0:${PORT}`);
});
