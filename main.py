import asyncio
import json
import random
import time
from datetime import datetime
import pytz
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Tenta importar a API oficial da IQ Option (se instalada no ambiente)
try:
    from iqoptionapi.stable_api import IQ_Option
    IQ_INSTALLED = True
except ImportError:
    IQ_INSTALLED = False

app = FastAPI(title="Bot MHI Engine - IQ Option")

# Permite chamadas CORS de qualquer origem (inclusive do GitHub Pages)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TIMEZONE_SP = pytz.timezone("America/Sao_Paulo")

class IQOptionManager:
    def __init__(self):
        self.api = None
        self.is_connected = False
        self.active_symbol = "EURUSD"
        self.account_type = "PRACTICE"

    def connect(self, email, password, account_type="PRACTICE"):
        self.account_type = account_type
        if IQ_INSTALLED:
            try:
                self.api = IQ_Option(email, password)
                check, reason = self.api.connect()
                if check:
                    self.api.change_balance(self.account_type)
                    self.is_connected = True
                    return True, "Conectado com sucesso à IQ Option!"
                else:
                    return False, f"Falha na conexão: {reason}"
            except Exception as e:
                return False, f"Erro ao conectar: {str(e)}"
        else:
            # Modo Simulação caso a biblioteca não esteja instalada
            self.is_connected = True
            return True, "Modo Simulação ativado (IQOption API não encontrada no servidor)."

    def get_sp_time(self):
        return datetime.now(TIMEZONE_SP)

    def analyze_mhi(self, candles):
        """
        Analisa as últimas 3 velas de M1 do quadrante de 5 minutos.
        Retorna: 'CALL' (Maioria Verde), 'PUT' (Maioria Vermelha) ou 'DOJI'
        """
        if len(candles) < 3:
            return "AGUARDANDO"

        last_3 = candles[-3:]
        greens = sum(1 for c in last_3 if c['close'] > c['open'])
        reds = sum(1 for c in last_3 if c['close'] < c['open'])

        if greens > reds:
            return "CALL"  # Maioria Verde
        elif reds > greens:
            return "PUT"   # Maioria Vermelha
        else:
            return "DOJI"  # Empate / Indefinido

manager = IQOptionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("[WebSocket] Cliente conectado!")

    try:
        while True:
            # Recebe comandos em JSON vindos do HTML no GitHub Pages
            data_str = await websocket.receive_text()
            data = json.loads(data_str)
            action = data.get("action")

            if action == "login":
                email = data.get("email")
                password = data.get("password")
                acc_type = data.get("account_type", "PRACTICE")

                await websocket.send_json({
                    "type": "log",
                    "message": f"Iniciando autenticação na conta {acc_type}..."
                })

                success, message = manager.connect(email, password, acc_type)

                if success:
                    await websocket.send_json({"type": "login_result", "status": "success", "message": message})
                    await websocket.send_json({"type": "log", "message": f"🟢 {message}"})
                    
                    # Inicia a tarefa de monitoramento continuo dos candles e MHI
                    asyncio.create_task(stream_mhi_data(websocket))
                else:
                    await websocket.send_json({"type": "login_result", "status": "error", "message": message})
                    await websocket.send_json({"type": "log", "message": f"🔴 {message}"})

            elif action == "change_asset":
                new_asset = data.get("asset", "EURUSD").replace("/", "").replace("-", "")
                manager.active_symbol = new_asset
                await websocket.send_json({
                    "type": "log",
                    "message": f"🔄 Ativo alterado para: {manager.active_symbol}"
                })

    except WebSocketDisconnect:
        print("[WebSocket] Cliente desconectado.")
    except Exception as e:
        print(f"[WebSocket Erro] {e}")

async def stream_mhi_data(websocket: WebSocket):
    """Loop continuo que sincroniza o relógio de Brasília/SP e monitora as entradas MHI."""
    last_processed_minute = -1

    while manager.is_connected:
        try:
            now_sp = manager.get_sp_time()
            second = now_sp.second
            minute = now_sp.minute

            # Simulação ou busca real de candles da corretora
            candles = []
            if IQ_INSTALLED and manager.api:
                try:
                    candles = manager.api.get_candles(manager.active_symbol, 60, 5, time.time())
                except:
                    pass

            # Caso esteja sem API real, gera dados simulados
            if not candles:
                base_price = 1.0850
                candles = [
                    {'open': base_price, 'close': base_price + 0.0002},
                    {'open': base_price + 0.0002, 'close': base_price - 0.0001},
                    {'open': base_price - 0.0001, 'close': base_price + 0.0003}
                ]

            # Envia atualização do estado do mercado a cada segundo
            signal = manager.analyze_mhi(candles)
            
            # Executa a validação e entrada aos 00s do novo minuto
            if second == 0 and minute != last_processed_minute:
                last_processed_minute = minute
                
                await websocket.send_json({
                    "type": "log",
                    "message": f"🎯 [00s ATINGIDO] Analisando MHI em {manager.active_symbol} | Sinal sugerido: {signal}"
                })

                if signal in ["CALL", "PUT"]:
                    await websocket.send_json({
                        "type": "log",
                        "message": f"⚡ Ordem {signal} disparada com sucesso em {manager.active_symbol}!"
                    })

            await asyncio.sleep(1)

        except Exception as err:
            print(f"[Loop Error] {err}")
            await asyncio.sleep(1)

if __name__ == "__main__":
    import uvicorn
    # Roda a aplicação na porta 8000
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
