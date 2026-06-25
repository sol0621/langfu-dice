/**
 * 郎府摇骰子 - WebSocket 服务端
 * Node.js + ws 库，纯内存状态管理
 */

const { WebSocketServer } = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

// ========================== 常量 ==========================
const PORT = process.env.PORT || 3000;
const GAME_MODES = ['dice_poker', 'guess_size', 'twenty_one'];
const DICE_COUNT = { dice_poker: 5, guess_size: 3, twenty_one: 2 };
const BUST_LIMIT = 21;

// ========================== 工具函数 ==========================
function genId() { return Math.random().toString(36).slice(2, 8); }
function rollDice(n) { return Array.from({ length: n }, () => Math.floor(Math.random() * 6) + 1); }

// ========================== 房间存储 ==========================
const rooms = new Map();

function getRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room) throw new Error('ROOM_NOT_FOUND');
  return room;
}

// ========================== HTTP + 静态服务 ==========================
const server = http.createServer((req, res) => {
  let filePath = req.url === '/' ? '/index.html' : req.url;
  filePath = path.join(__dirname, 'public', filePath);

  const extMap = { '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript', '.json': 'application/json', '.png': 'image/png' };
  const ext = path.extname(filePath);
  const contentType = extMap[ext] || 'text/plain';

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not Found'); return; }
    res.writeHead(200, { 'Content-Type': contentType });
    res.end(data);
  });
});

// ========================== WebSocket ==========================
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.userId = genId();
  ws.roomId = null;
  ws.nickname = null;

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      handleMessage(ws, msg);
    } catch (e) {
      send(ws, { type: 'error', data: { message: '消息格式错误' } });
    }
  });

  ws.on('close', () => handleDisconnect(ws));
});

// ========================== 消息路由 ==========================
function handleMessage(ws, msg) {
  const { type, data } = msg;
  try {
    switch (type) {
      case 'create_room': handleCreateRoom(ws, data); break;
      case 'join_room': handleJoinRoom(ws, data); break;
      case 'leave_room': handleLeaveRoom(ws); break;
      case 'start_game': handleStartGame(ws); break;
      case 'call_dice': handleCallDice(ws, data); break;
      case 'challenge': handleChallenge(ws); break;
      case 'guess': handleGuess(ws, data); break;
      case 'hit': handleHit(ws); break;
      case 'stand': handleStand(ws); break;
      case 'next_round': handleNextRound(ws); break;
      case 'switch_mode': handleSwitchMode(ws, data); break;
      default: send(ws, { type: 'error', data: { message: `未知消息类型: ${type}` } });
    }
  } catch (e) {
    send(ws, { type: 'error', data: { message: e.message } });
  }
}

// ========================== 发送函数 ==========================
function send(ws, msg) { if (ws.readyState === 1) ws.send(JSON.stringify(msg)); }

function broadcast(roomId, msg, excludeUserId) {
  const room = rooms.get(roomId);
  if (!room) return;
  room.players.forEach(p => {
    if (p.ws.readyState === 1 && p.userId !== excludeUserId) send(p.ws, msg);
  });
}

function broadcastAll(roomId, msg) { broadcast(roomId, msg, null); }

function buildRoomUpdate(roomId) {
  const room = rooms.get(roomId);
  if (!room) return null;
  return {
    type: 'room_update',
    data: {
      roomId: room.id,
      status: room.status,
      mode: room.mode,
      hostId: room.hostId,
      currentRound: room.currentRound,
      lastCall: room.lastCall || null,
      lastCallerId: room.lastCallerId || null,
      publicDice: room.publicDice || null,
      players: room.players.map(p => ({
        userId: p.userId,
        nickname: p.nickname,
        isAlive: p.isAlive,
        isStopped: p.isStopped,
        isBusted: p.isBusted,
        total: p.total,
        diceCount: p.diceValues ? p.diceValues.length : 0
      }))
    }
  };
}

// ========================== 房间操作 ==========================
function handleCreateRoom(ws, data) {
  if (ws.roomId) throw new Error('ALREADY_IN_ROOM');
  const mode = GAME_MODES.includes(data.mode) ? data.mode : 'dice_poker';
  const roomId = genId();
  ws.nickname = data.nickname || '玩家';

  const room = {
    id: roomId,
    mode: mode,
    status: 'idle',
    hostId: ws.userId,
    currentRound: 0,
    lastCall: null,
    lastCallerId: null,
    publicDice: null,
    turnIndex: 0,
    players: []
  };

  addPlayerToRoom(room, ws);
  rooms.set(roomId, room);
  ws.roomId = roomId;

  send(ws, { type: 'room_created', data: { roomId } });
  broadcastAll(roomId, buildRoomUpdate(roomId));
}

function handleJoinRoom(ws, data) {
  if (ws.roomId) throw new Error('ALREADY_IN_ROOM');
  ws.nickname = data.nickname || '玩家';
  const room = getRoom(data.roomId);

  if (room.status !== 'idle') throw new Error('GAME_IN_PROGRESS');
  if (room.players.length >= 8) throw new Error('ROOM_FULL');

  addPlayerToRoom(room, ws);
  ws.roomId = room.id;
  send(ws, { type: 'room_created', data: { roomId: room.id } });
  broadcastAll(room.id, buildRoomUpdate(room.id));
}

function handleLeaveRoom(ws) {
  if (!ws.roomId) return;
  const room = rooms.get(ws.roomId);
  if (!room) { ws.roomId = null; return; }

  removePlayerFromRoom(room, ws.userId);
  ws.roomId = null;

  if (room.players.length === 0) {
    rooms.delete(room.id);
  } else {
    if (room.hostId === ws.userId) room.hostId = room.players[0].userId;
    broadcastAll(room.id, buildRoomUpdate(room.id));
  }
}

function handleDisconnect(ws) {
  if (ws.roomId) {
    const room = rooms.get(ws.roomId);
    if (room) {
      const player = room.players.find(p => p.userId === ws.userId);
      if (player) {
        player.ws = null;
        player.disconnected = true;
        setTimeout(() => {
          if (player.disconnected && rooms.has(ws.roomId)) {
            removePlayerFromRoom(room, ws.userId);
            if (room.players.length === 0) rooms.delete(room.id);
            else broadcastAll(room.id, buildRoomUpdate(room.id));
          }
        }, 5 * 60 * 1000);
      }
    }
  }
}

function addPlayerToRoom(room, ws) {
  room.players.push({
    userId: ws.userId,
    nickname: ws.nickname,
    ws: ws,
    isAlive: true,
    isStopped: false,
    isBusted: false,
    diceValues: [],
    total: 0,
    disconnected: false
  });
}

function removePlayerFromRoom(room, userId) {
  const idx = room.players.findIndex(p => p.userId === userId);
  if (idx !== -1) room.players.splice(idx, 1);
}

// ========================== 游戏开始 ==========================
function handleStartGame(ws) {
  const room = getRoom(ws.roomId);
  if (room.hostId !== ws.userId) throw new Error('NOT_HOST');
  if (room.players.length < 2) throw new Error('NOT_ENOUGH_PLAYERS');
  if (room.status !== 'idle' && room.status !== 'gameover') throw new Error('INVALID_STATUS');

  room.status = 'rolling';
  room.currentRound++;
  room.turnIndex = 0;
  room.lastCall = null;
  room.lastCallerId = null;

  // 重置所有玩家
  room.players.forEach(p => {
    p.isAlive = true;
    p.isStopped = false;
    p.isBusted = false;
    p.diceValues = [];
    p.total = 0;
  });

  broadcastAll(room.id, { type: 'game_started', data: { mode: room.mode, round: room.currentRound } });
  broadcastAll(room.id, buildRoomUpdate(room.id));

  // 2秒摇骰动画后发骰
  setTimeout(() => {
    if (!rooms.has(room.id)) return;
    room.status = 'playing';

    if (room.mode === 'guess_size') {
      room.publicDice = rollDice(3);
    } else if (room.mode === 'dice_poker') {
      room.players.forEach(p => {
        if (p.isAlive) p.diceValues = rollDice(DICE_COUNT.dice_poker);
      });
    } else if (room.mode === 'twenty_one') {
      room.players.forEach(p => {
        if (p.isAlive) {
          p.diceValues = rollDice(DICE_COUNT.twenty_one);
          p.total = p.diceValues.reduce((a, b) => a + b, 0);
          p.isStopped = false;
          p.isBusted = p.total > BUST_LIMIT;
          if (p.isBusted) p.isAlive = false;
        }
      });
    }

    // 发送骰子（点对点，猜大小模式发给所有人公共骰子）
    if (room.mode === 'guess_size') {
      broadcastAll(room.id, { type: 'dice_result', data: { values: room.publicDice } });
    } else {
      room.players.forEach(p => {
        if (p.ws && p.ws.readyState === 1) {
          send(p.ws, { type: 'dice_result', data: { values: p.diceValues, total: p.total } });
        }
      });
    }

    broadcastAll(room.id, buildRoomUpdate(room.id));

    // 21点：检查是否全员爆掉
    if (room.mode === 'twenty_one') {
      const alivePlayers = room.players.filter(p => p.isAlive && !p.isBusted);
      if (alivePlayers.length === 0) {
        endGame(room, null, '本局无人获胜——全员爆掉！');
        return;
      }
      if (alivePlayers.length === 1) {
        endGame(room, alivePlayers[0].userId, `${alivePlayers[0].nickname} 不战而胜！`);
        return;
      }
    }
  }, 2000);
}

// ========================== 游戏逻辑 ==========================

// --- 大话骰 ---
function handleCallDice(ws, data) {
  const room = getRoom(ws.roomId);
  if (room.mode !== 'dice_poker') throw new Error('WRONG_MODE');
  if (room.status !== 'playing') throw new Error('NOT_PLAYING');

  const player = room.players.find(p => p.userId === ws.userId);
  if (!player || !player.isAlive) throw new Error('NOT_ALIVE');

  // 必须轮到该玩家
  const alivePlayers = room.players.filter(p => p.isAlive);
  const turnPlayer = alivePlayers[room.turnIndex % alivePlayers.length];
  if (turnPlayer.userId !== ws.userId) throw new Error('NOT_YOUR_TURN');

  const { count, face } = data;
  const totalDice = alivePlayers.reduce((sum, p) => sum + p.diceValues.length, 0);

  if (count < 1 || count > totalDice) throw new Error('INVALID_COUNT');
  if (face < 2 || face > 6) throw new Error('INVALID_FACE');

  // 检查叫法必须大于上一个
  if (room.lastCall) {
    const { count: lc, face: lf } = room.lastCall;
    if (face === 1) {
      // 叫1只能被更大的1压
      if (lf === 1 && count <= lc) throw new Error('CALL_TOO_LOW');
    } else if (lf === 1) {
      // 上家叫1，本家叫非1需要 count >= lc*2+1（翻倍）
      if (count < lc * 2 + 1) throw new Error('CALL_TOO_LOW');
    } else {
      if (count < lc || (count === lc && face <= lf)) throw new Error('CALL_TOO_LOW');
    }
  }

  room.lastCall = { count, face };
  room.lastCallerId = ws.userId;
  room.turnIndex = (room.turnIndex + 1) % alivePlayers.length;

  broadcastAll(room.id, {
    type: 'call_made',
    data: { userId: ws.userId, nickname: player.nickname, count, face }
  });
  broadcastAll(room.id, buildRoomUpdate(room.id));
}

function handleChallenge(ws) {
  const room = getRoom(ws.roomId);
  if (room.mode !== 'dice_poker') throw new Error('WRONG_MODE');
  if (room.status !== 'playing') throw new Error('NOT_PLAYING');
  if (!room.lastCall) throw new Error('NOTHING_TO_CHALLENGE');

  const challenger = room.players.find(p => p.userId === ws.userId);
  if (!challenger || !challenger.isAlive) throw new Error('NOT_ALIVE');

  // 不能质疑自己
  if (room.lastCallerId === ws.userId) throw new Error('CANT_CHALLENGE_SELF');

  room.status = 'revealing';
  const called = room.lastCall;
  const calledPlayer = room.players.find(p => p.userId === room.lastCallerId);

  // 统计所有存活玩家的骰子
  const alivePlayers = room.players.filter(p => p.isAlive);
  let actualCount = 0;
  alivePlayers.forEach(p => {
    p.diceValues.forEach(v => {
      if (v === called.face || v === 1) actualCount++; // 万能点
    });
  });

  const correct = actualCount >= called.count;
  const loserId = correct ? ws.userId : room.lastCallerId;
  const loser = room.players.find(p => p.userId === loserId);

  if (loser) {
    loser.isAlive = false;
    loser.diceValues = []; // 淘汰清空骰子
  }

  // 广播开骰结果（包含所有骰子）
  broadcastAll(room.id, {
    type: 'challenge_result',
    data: {
      challengerId: challenger.userId,
      challengerNickname: challenger.nickname,
      calledPlayerId: calledPlayer.userId,
      calledPlayerNickname: calledPlayer.nickname,
      calledCount: called.count,
      calledFace: called.face,
      actualCount,
      correct,
      loserId,
      loserNickname: loser ? loser.nickname : '',
      allDice: alivePlayers.map(p => ({
        userId: p.userId,
        nickname: p.nickname,
        values: [...p.diceValues]
      }))
    }
  });

  // 检查胜负
  const remaining = room.players.filter(p => p.isAlive);
  if (remaining.length <= 1) {
    endGame(room, remaining[0] ? remaining[0].userId : null,
      remaining[0] ? `${remaining[0].nickname} 获胜！` : '平局！');
  } else {
    room.status = 'gameover';
    room.lastCall = null;
    room.lastCallerId = null;
    room.turnIndex = 0;
    broadcastAll(room.id, buildRoomUpdate(room.id));
  }
}

// --- 猜大小 ---
function handleGuess(ws, data) {
  const room = getRoom(ws.roomId);
  if (room.mode !== 'guess_size') throw new Error('WRONG_MODE');
  if (room.status !== 'playing') throw new Error('NOT_PLAYING');

  const player = room.players.find(p => p.userId === ws.userId);
  if (!player || !player.isAlive) throw new Error('NOT_ALIVE');
  if (player.guess !== undefined) throw new Error('ALREADY_GUESSED');

  const guessType = data.type;
  if (!['big', 'small'].includes(guessType)) throw new Error('INVALID_GUESS');

  player.guess = guessType;

  // 检查是否所有人都猜了
  const alivePlayers = room.players.filter(p => p.isAlive);
  const allGuessed = alivePlayers.every(p => p.guess !== undefined);

  if (allGuessed) resolveGuessSize(room);
  else broadcastAll(room.id, buildRoomUpdate(room.id));
}

function resolveGuessSize(room) {
  room.status = 'revealing';
  const dice = room.publicDice;
  const sum = dice.reduce((a, b) => a + b, 0);
  const isTriple = dice[0] === dice[1] && dice[1] === dice[2]; // 围骰
  const isBig = sum >= 11 && sum <= 17;
  const isSmall = sum >= 4 && sum <= 10;

  const alivePlayers = room.players.filter(p => p.isAlive);
  const guesses = alivePlayers.map(p => ({ userId: p.userId, nickname: p.nickname, guess: p.guess }));
  const losers = [];

  alivePlayers.forEach(p => {
    if (isTriple) {
      p.isAlive = false;
      losers.push(p.userId);
    } else {
      const correct = (p.guess === 'big' && isBig) || (p.guess === 'small' && isSmall);
      if (!correct) {
        p.isAlive = false;
        losers.push(p.userId);
      }
    }
    p.guess = undefined; // 重置
  });

  broadcastAll(room.id, {
    type: 'guess_result',
    data: {
      diceValues: dice,
      sum,
      isTriple,
      isBig,
      isSmall,
      guesses,
      losers,
      losersNickname: losers.map(id => {
        const p = room.players.find(pl => pl.userId === id);
        return p ? p.nickname : '';
      })
    }
  });

  const remaining = room.players.filter(p => p.isAlive);
  if (remaining.length <= 1) {
    endGame(room, remaining[0] ? remaining[0].userId : null,
      isTriple ? '围骰！全员淘汰！' : remaining[0] ? `${remaining[0].nickname} 获胜！` : '全员淘汰！');
  } else {
    room.status = 'gameover';
    broadcastAll(room.id, buildRoomUpdate(room.id));
  }
}

// --- 21点 ---
function handleHit(ws) {
  const room = getRoom(ws.roomId);
  if (room.mode !== 'twenty_one') throw new Error('WRONG_MODE');
  if (room.status !== 'playing') throw new Error('NOT_PLAYING');

  const player = room.players.find(p => p.userId === ws.userId);
  if (!player || !player.isAlive || player.isStopped || player.isBusted) throw new Error('CANT_HIT');

  const newDie = rollDice(1)[0];
  player.diceValues.push(newDie);
  player.total = player.diceValues.reduce((a, b) => a + b, 0);

  if (player.total > BUST_LIMIT) {
    player.isBusted = true;
    player.isAlive = false;
  }

  send(ws, { type: 'dice_result', data: { values: player.diceValues, total: player.total } });

  const data = { userId: player.userId, nickname: player.nickname, total: player.total, isBusted: player.isBusted };
  broadcastAll(room.id, { type: 'hit_result', data });

  checkTwentyOneDone(room);
}

function handleStand(ws) {
  const room = getRoom(ws.roomId);
  if (room.mode !== 'twenty_one') throw new Error('WRONG_MODE');
  if (room.status !== 'playing') throw new Error('NOT_PLAYING');

  const player = room.players.find(p => p.userId === ws.userId);
  if (!player || !player.isAlive || player.isBusted) throw new Error('CANT_STAND');
  if (player.isStopped) throw new Error('ALREADY_STOPPED');

  player.isStopped = true;
  broadcastAll(room.id, { type: 'stand_result', data: { userId: player.userId, nickname: player.nickname, total: player.total } });
  broadcastAll(room.id, buildRoomUpdate(room.id));

  checkTwentyOneDone(room);
}

function checkTwentyOneDone(room) {
  const activePlayers = room.players.filter(p => p.isAlive && !p.isStopped && !p.isBusted);
  if (activePlayers.length === 0) resolveTwentyOne(room);
}

function resolveTwentyOne(room) {
  room.status = 'revealing';
  const players = room.players.filter(p => !p.isBusted || p.diceValues.length > 0);

  // 所有未爆的玩家中总分最高者胜
  const notBusted = players.filter(p => !p.isBusted);
  if (notBusted.length === 0) {
    endGame(room, null, '本局无人获胜——全员爆掉！');
    return;
  }

  const maxTotal = Math.max(...notBusted.map(p => p.total));
  const winners = notBusted.filter(p => p.total === maxTotal);

  broadcastAll(room.id, {
    type: 'twenty_one_result',
    data: {
      players: players.map(p => ({
        userId: p.userId,
        nickname: p.nickname,
        values: p.diceValues,
        total: p.total,
        isBusted: p.isBusted
      })),
      winnerIds: winners.map(w => w.userId)
    }
  });

  if (winners.length === 1) {
    endGame(room, winners[0].userId, `${winners[0].nickname} 以 ${winners[0].total} 点获胜！`);
  } else {
    endGame(room, null, `平局！${winners.map(w => w.nickname).join('、')} 均为 ${maxTotal} 点`);
  }
}

// ========================== 游戏结束 ==========================
function endGame(room, winnerId, message) {
  room.status = 'gameover';
  room.lastCall = null;
  room.lastCallerId = null;
  room.turnIndex = 0;

  broadcastAll(room.id, {
    type: 'game_over',
    data: { winnerId, message }
  });
  broadcastAll(room.id, buildRoomUpdate(room.id));
}

// ========================== 下一局 / 模式切换 ==========================
function handleNextRound(ws) {
  const room = getRoom(ws.roomId);
  if (room.hostId !== ws.userId) throw new Error('NOT_HOST');
  if (room.status !== 'gameover') throw new Error('NOT_GAMEOVER');
  room.status = 'idle';
  broadcastAll(room.id, buildRoomUpdate(room.id));
}

function handleSwitchMode(ws, data) {
  const room = getRoom(ws.roomId);
  if (room.hostId !== ws.userId) throw new Error('NOT_HOST');
  if (room.status !== 'idle' && room.status !== 'gameover') throw new Error('GAME_IN_PROGRESS');

  const mode = data.mode;
  if (!GAME_MODES.includes(mode)) throw new Error('INVALID_MODE');

  room.mode = mode;
  room.lastCall = null;
  room.lastCallerId = null;
  room.publicDice = null;
  room.status = 'idle';

  broadcastAll(room.id, buildRoomUpdate(room.id));
}

// ========================== 启动 ==========================
server.listen(PORT, () => {
  console.log(`🎲 郎府摇骰子 服务端已启动: http://localhost:${PORT}`);
});
