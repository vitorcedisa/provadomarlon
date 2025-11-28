"""
API Gateway Simulado
Simula roteamento de requisições, autenticação básica e rate limiting.
"""
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from colorama import Fore, Style, init

init(autoreset=True)


class APIGateway:
    """Simula um API Gateway com roteamento, autenticação e rate limiting."""

    def __init__(self):
        self.request_log: list = []
        self.rate_limit: Dict[str, list] = {}  # IP -> [timestamps]
        self.rate_limit_window = 60  # segundos
        self.max_requests_per_window = 100

    def _check_rate_limit(self, client_ip: str) -> bool:
        """Verifica se o cliente excedeu o rate limit."""
        now = time.time()
        if client_ip not in self.rate_limit:
            self.rate_limit[client_ip] = []

        # Remove timestamps antigos
        self.rate_limit[client_ip] = [
            ts for ts in self.rate_limit[client_ip] if now - ts < self.rate_limit_window
        ]

        if len(self.rate_limit[client_ip]) >= self.max_requests_per_window:
            return False

        self.rate_limit[client_ip].append(now)
        return True

    def _log_request(self, method: str, path: str, client_ip: str, status: int):
        """Registra a requisição no log."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "method": method,
            "path": path,
            "client_ip": client_ip,
            "status": status,
        }
        self.request_log.append(log_entry)
        print(
            Fore.LIGHTBLUE_EX
            + f"[API Gateway] {method} {path} - IP: {client_ip} - Status: {status}"
        )

    def _check_auth(self, headers: Dict[str, Any]) -> bool:
        """Simula verificação de autenticação básica."""
        # Em produção, aqui verificaria tokens JWT, API keys, etc.
        # Para simulação, aceita qualquer requisição
        api_key = headers.get("X-API-Key") or headers.get("Authorization")
        if api_key:
            print(Fore.GREEN + "[API Gateway] Autenticação verificada via API Key.")
            return True
        # Permite requisições sem autenticação para simplicidade
        return True

    def route(
        self, method: str, path: str, handler: Callable, client_ip: str = "127.0.0.1", headers: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Roteia uma requisição através do gateway.
        Retorna a resposta do handler ou erro do gateway.
        """
        headers = headers or {}

        # Verifica rate limit
        if not self._check_rate_limit(client_ip):
            self._log_request(method, path, client_ip, 429)
            return {
                "status_code": 429,
                "body": {"erro": "Rate limit excedido. Tente novamente mais tarde."},
            }

        # Verifica autenticação
        if not self._check_auth(headers):
            self._log_request(method, path, client_ip, 401)
            return {
                "status_code": 401,
                "body": {"erro": "Não autorizado. Forneça credenciais válidas."},
            }

        # Roteia para o handler
        try:
            print(Fore.CYAN + f"[API Gateway] Roteando {method} {path} para handler.")
            response = handler()
            status = response[1] if isinstance(response, tuple) else 200
            self._log_request(method, path, client_ip, status)
            return {"status_code": status, "body": response[0] if isinstance(response, tuple) else response}
        except Exception as e:
            self._log_request(method, path, client_ip, 500)
            return {
                "status_code": 500,
                "body": {"erro": f"Erro interno do servidor: {str(e)}"},
            }

    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas do gateway."""
        total_requests = len(self.request_log)
        
        # Conta requisições da última hora (simplificado)
        now = time.time()
        recent_requests = [
            req for req in self.request_log
            if req.get("timestamp")  # Apenas verifica se existe timestamp
        ]

        status_counts = {}
        for req in self.request_log:
            status = req["status"]
            status_counts[status] = status_counts.get(status, 0) + 1

        return {
            "total_requests": total_requests,
            "requests_last_hour": len(recent_requests),
            "status_counts": status_counts,
            "active_clients": len(self.rate_limit),
        }


# Instância global do gateway
api_gateway = APIGateway()

