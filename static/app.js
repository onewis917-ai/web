(() => {

  const $ = (sel) => document.querySelector(sel);

  const tokensEl = $("#tokens");
  const btnAdd = $("#btn-add");
  const btnStart = $("#btn-start");
  const btnStop = $("#btn-stop");
  const btnClear = $("#btn-clear");
  const btnClearLogs = $("#btn-clear-logs");
  const btnRefreshClaimed = $("#btn-refresh-claimed");
  const logBox = $("#log-box");
  const engineStatus = $("#engine-status");
  const enginePill = $("#engine-pill");
  const engineIndicator = $("#engine-indicator");
  const accountCount = $("#account-count");
  const claimedCount = $("#claimed-count");
  const accountList = $("#account-list");
  const accountCountLabel = $("#account-count-label");
  const claimedBody = $("#claimed-body");
  const wsDot = $("#ws-dot");
  const toastWrap = $("#toast-wrap");

  let ws = null;
  let reconnectTimer = null;
  let logEmpty = true;




  async function api(method, path, body) {
    const opts = { method, headers: { "Content-Type": "application/json" } };
    if (body !== undefined) opts.body = JSON.stringify(body);
    const res = await fetch(path, opts);
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.detail || res.statusText);
    return data;
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function now8() {
    return new Date().toTimeString().slice(0, 8);
  }



  function showToast(msg, type = "info") {
    if (!toastWrap) return;
    const el = document.createElement("div");
    el.className = "toast";
    const colors = {
      ok: "var(--green)",
      error: "var(--red)",
      warn: "var(--yellow)",
      info: "var(--purple)",
    };
    el.style.borderLeftColor = colors[type] || colors.info;
    el.style.borderLeftWidth = "3px";
    el.style.borderLeftStyle = "solid";
    el.textContent = msg;
    toastWrap.appendChild(el);
    setTimeout(() => el.remove(), 3500);
  }




  function appendLog(entry) {

    if (logEmpty) {
      logBox.innerHTML = "";
      logEmpty = false;
    }
    const line = document.createElement("div");
    line.className = "log-line";
    const lvl = entry.level || "info";
    const label = (lvl.toUpperCase() + "    ").slice(0, 4);
    line.innerHTML =
      `<span class="log-ts">${escapeHtml(entry.ts || "")}</span>` +
      `<span class="log-level lvl-${escapeHtml(lvl)}">${label}</span>` +
      `<span class="log-msg">${escapeHtml(entry.msg || "")}</span>`;
    logBox.appendChild(line);


    while (logBox.children.length > 400) logBox.removeChild(logBox.firstChild);
    logBox.scrollTop = logBox.scrollHeight;
  }




  function setWsState(online) {
    wsDot.className = "ws-dot " + (online ? "online" : "offline");
  }


  async function refreshStatus() {
    try {
      const st = await api("GET", "/api/status");
      const running = !!st.running;
      const count = st.accounts ? st.accounts.length : 0;


      engineStatus.textContent = running ? "ON" : "OFF";
      engineStatus.className = "stat-val " + (running ? "mint" : "dim");


      enginePill.textContent = running ? "ONLINE" : "OFFLINE";
      enginePill.className = "engine-pill " + (running ? "online" : "offline");


      if (running) {
        engineIndicator.classList.add("active");
      } else {
        engineIndicator.classList.remove("active");
      }


      accountCount.textContent = count;
      accountCountLabel.textContent = count;
      claimedCount.textContent = st.claimed_count ?? 0;


      if (count > 0) {
        accountList.innerHTML = st.accounts
          .map((a) => `<span class="chip">@${escapeHtml(a.username)}</span>`)
          .join("");
      } else {
        accountList.innerHTML = '<span class="chip empty">ยังไม่มีบัญชี</span>';
      }


      btnStart.disabled = running || count === 0;
      btnStop.disabled = !running;

    } catch (e) {

    }
  }



  async function refreshClaimed() {
    try {
      const data = await api("GET", "/api/claimed");
      const rows = data.claimed || [];
      if (!rows.length) {
        claimedBody.innerHTML =
          `<tr class="empty-row"><td colspan="4">
            <div class="table-empty">
              <span class="table-empty-icon" aria-hidden="true">◇</span>
              <span>ยังไม่มี Reward ที่ claim</span>
            </div>
          </td></tr>`;
        return;
      }
      claimedBody.innerHTML = rows
        .map(
          (r) => `<tr>
            <td>${escapeHtml(r.timestamp || "")}</td>
            <td>${escapeHtml(r.account || "")}</td>
            <td>${escapeHtml(r.quest || "")}</td>
            <td>${escapeHtml(r.code || "")}</td>
          </tr>`
        )
        .join("");
    } catch (e) {

    }
  }




  function connectWs() {
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
    const proto = location.protocol === "https:" ? "wss" : "ws";
    ws = new WebSocket(`${proto}://${location.host}/ws`);

    ws.onopen = () => {
      setWsState(true);
    };

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg.type === "log") {
          appendLog(msg.data);
          if (msg.data.level === "gift") {
            refreshClaimed();
            refreshStatus();
            showToast("🎉 Reward claimed!", "ok");
          }
          if (msg.data.level === "ok") {
            refreshStatus();
          }
        }
      } catch (e) { }
    };

    ws.onclose = () => {
      setWsState(false);
      clearTimeout(reconnectTimer);

      const delay = Math.min(10000, 2500 * (reconnectAttempts + 1));
      reconnectAttempts++;
      reconnectTimer = setTimeout(() => {
        reconnectAttempts = Math.max(0, reconnectAttempts - 1);
        connectWs();
      }, delay);
    };

    ws.onerror = () => {
      try { ws.close(); } catch (e) { }
    };
  }

  let reconnectAttempts = 0;


  setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      try { ws.send("ping"); } catch (e) { }
    }
  }, 25000);




  btnAdd.addEventListener("click", async () => {
    const raw = tokensEl.value.trim();
    if (!raw) return;
    const tokens = raw.split(/\r?\n/).map((t) => t.trim()).filter(Boolean);
    btnAdd.disabled = true;
    const prevHtml = btnAdd.innerHTML;
    btnAdd.innerHTML = `<span class="btn-ico">⟳</span> กำลังตรวจสอบ...`;

    try {
      const result = await api("POST", "/api/tokens", { tokens });
      if (result.ok && result.ok.length) {
        tokensEl.value = "";
        const names = result.ok.map((a) => "@" + a.username).join(", ");
        const sent = result.ok.filter((a) => a.webhook_sent).length;
        
        
        showToast(`เพิ่ม ${result.ok.length} บัญชีสำเร็จ`, "ok");
      }
      if (result.failed && result.failed.length) {
        appendLog({
          ts: now8(),
          level: "error",
          msg: `Token ไม่ผ่านการตรวจสอบ: ${result.failed.length} รายการ`,
        });
        showToast(`Token ไม่ผ่าน ${result.failed.length} รายการ`, "error");
      }
      await refreshStatus();
    } catch (e) {
      appendLog({ ts: now8(), level: "error", msg: String(e.message || e) });
      showToast(String(e.message || e), "error");
    } finally {
      btnAdd.disabled = false;
      btnAdd.innerHTML = prevHtml;
    }
  });


  btnStart.addEventListener("click", async () => {
    btnStart.disabled = true;
    try {
      await api("POST", "/api/start");
      appendLog({ ts: now8(), level: "ok", msg: "Engine started ✓" });
      showToast("Engine started", "ok");
      await refreshStatus();
    } catch (e) {
      appendLog({ ts: now8(), level: "error", msg: String(e.message || e) });
      showToast(String(e.message || e), "error");
      btnStart.disabled = false;
    }
  });


  btnStop.addEventListener("click", async () => {
    btnStop.disabled = true;
    try {
      await api("POST", "/api/stop");
      appendLog({ ts: now8(), level: "warn", msg: "Engine stop requested" });
      showToast("Engine หยุดแล้ว", "warn");
      await refreshStatus();
    } catch (e) {
      appendLog({ ts: now8(), level: "error", msg: String(e.message || e) });
      btnStop.disabled = false;
    }
  });


  btnClear.addEventListener("click", async () => {
    try {
      await api("DELETE", "/api/accounts");
      appendLog({ ts: now8(), level: "info", msg: "ล้างบัญชีทั้งหมดแล้ว" });
      showToast("ล้างบัญชีแล้ว", "info");
      await refreshStatus();
    } catch (e) {
      appendLog({ ts: now8(), level: "error", msg: String(e.message || e) });
    }
  });


  btnClearLogs.addEventListener("click", () => {
    logBox.innerHTML = "";
    logEmpty = true;
    logBox.innerHTML = `
      <div class="log-empty">
        <div class="log-empty-icon" aria-hidden="true">$_</div>
        <div>
          <strong>Log cleared</strong>
          <span>Activity will appear here in real-time</span>
        </div>
      </div>`;
  });


  btnRefreshClaimed.addEventListener("click", () => {
    refreshClaimed();
    showToast("รีเฟรชแล้ว", "info");
  });




  connectWs();
  refreshStatus();
  refreshClaimed();
  setInterval(refreshStatus, 5000);

})();
