import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from colorama import Fore, Style, init

# Inicia suporte a cores multiplataforma
init(autoreset=True)


class MockAWS:
    """
    Simulador extremamente simples dos serviços utilizados (SQS, SNS e Lambda).
    Toda a persistência fica em arquivos locais para permitir múltiplos processos.
    """

    def __init__(self, base_path: Optional[Path] = None) -> None:
        self.state_dir = Path(base_path or Path(__file__).parent / "mock_state")
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self.sqs_dir = self.state_dir / "sqs"
        self.sqs_dir.mkdir(exist_ok=True)

        self.sns_log = self.state_dir / "sns_log.txt"
        if not self.sns_log.exists():
            self.sns_log.write_text("", encoding="utf-8")

    def _queue_file(self, queue_name: str) -> Path:
        file_path = self.sqs_dir / f"{queue_name}.json"
        if not file_path.exists():
            file_path.write_text("[]", encoding="utf-8")
        return file_path

    def send_sqs(self, queue_name: str, message: Dict[str, Any]) -> None:
        """Simula o envio de mensagem para uma fila SQS."""
        file_path = self._queue_file(queue_name)
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        payload.append(message)
        file_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(
            Fore.CYAN
            + f"[AWS SQS] Mensagem adicionada na fila '{queue_name}': {json.dumps(message, ensure_ascii=False)}"
        )

    def receive_sqs(self, queue_name: str) -> Optional[Dict[str, Any]]:
        """Retira a primeira mensagem da fila simulada."""
        file_path = self._queue_file(queue_name)
        payload: List[Dict[str, Any]] = json.loads(file_path.read_text(encoding="utf-8"))
        if not payload:
            print(Fore.MAGENTA + f"[AWS SQS] Fila '{queue_name}' vazia...")
            return None

        message = payload.pop(0)
        file_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(Fore.MAGENTA + f"[AWS SQS] Mensagem recuperada da fila '{queue_name}': {json.dumps(message, ensure_ascii=False)}")
        return message

    def publish_sns(self, topic: str, message: str) -> None:
        """Simula o envio de uma notificação SNS."""
        log_entry = f"{datetime.utcnow().isoformat()}Z | {topic} | {message}\n"
        with self.sns_log.open("a", encoding="utf-8") as handler:
            handler.write(log_entry)
        print(Fore.YELLOW + f"[AWS SNS] Notificação enviada ao tópico '{topic}': {message}")

    def invoke_lambda(self, name: str, handler, payload: Dict[str, Any]) -> Any:
        """Simula a invocação de uma função Lambda."""
        print(Fore.GREEN + f"[AWS Lambda] Invocando '{name}' com payload: {json.dumps(payload, ensure_ascii=False)}")
        result = handler(payload, context={"invoked_at": datetime.utcnow().isoformat()})
        print(Fore.GREEN + f"[AWS Lambda] Execução '{name}' finalizada.\n")
        return result


# Instância única compartilhada pelos módulos
mock_aws = MockAWS()


def lambda_matchmaker(event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Recebe a lista de atletas e monta os confrontos da chave."""
    atletas: List[Dict[str, Any]] = event.get("atletas", [])
    if len(atletas) < 2:
        print(Fore.RED + "[Lambda Matchmaker] Número insuficiente de atletas para gerar chaves.")
        return {"confrontos": []}

    embaralhados = atletas[:]
    random.shuffle(embaralhados)

    confrontos = []
    for idx in range(0, len(embaralhados), 2):
        dupla = embaralhados[idx : idx + 2]
        luta_id = f"LUTA-{idx // 2 + 1}"
        if len(dupla) == 2:
            confrontos.append(
                {
                    "luta_id": luta_id,
                    "atletas": dupla,
                    "round": "Classificatórias",
                }
            )
        else:
            # Último atleta sem par recebe bye
            confrontos.append(
                {
                    "luta_id": f"{luta_id}-BYE",
                    "atletas": dupla,
                    "round": "Avanço Automático",
                }
            )

    print(
        Fore.BLUE
        + f"[Lambda Matchmaker] {len(confrontos)} confrontos gerados para {len(atletas)} atletas."
    )
    return {"confrontos": confrontos, "gerado_em": datetime.utcnow().isoformat() + "Z"}


def lambda_announcer(event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Processa mensagem da fila e dispara aviso (SNS)."""
    luta_id = event.get("luta_id", "LUTA-DESCONHECIDA")
    atletas = event.get("atletas", [])
    round_name = event.get("round", "Rodada Única")

    atletas_nomes = " vs ".join(a.get("nome", "??") for a in atletas) or "Participantes indefinidos"
    mensagem = f"{round_name} - {luta_id}: {atletas_nomes}. Dirijam-se ao tatame!"

    print(Fore.WHITE + Style.BRIGHT + f"[Lambda Announcer] Preparando anúncio da luta {luta_id}.")
    mock_aws.publish_sns(topic="jiujitsu-lutas", message=mensagem)
    time.sleep(0.5)  # Latência simbólica
    return {"status": "ANNOUNCED", "mensagem": mensagem}


def lambda_historian(event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Persiste o resultado em um log JSON (simulando backup S3)."""
    backup_dir = mock_aws.state_dir / "backups"
    backup_dir.mkdir(exist_ok=True)
    backup_file = backup_dir / "historian_logs.json"

    if not backup_file.exists():
        backup_file.write_text("[]", encoding="utf-8")

    log_entry = {
        "luta_id": event.get("luta_id"),
        "vencedor": event.get("vencedor"),
        "submitido_por": event.get("submitido_por", "N/A"),
        "registrado_em": datetime.utcnow().isoformat() + "Z",
        "extra": event.get("extra", {}),
    }

    dados = json.loads(backup_file.read_text(encoding="utf-8"))
    dados.append(log_entry)
    backup_file.write_text(json.dumps(dados, indent=2, ensure_ascii=False), encoding="utf-8")

    print(Fore.LIGHTBLACK_EX + f"[Lambda Historian] Resultado salvo em {backup_file}.")
    return {"status": "BACKED_UP", "arquivo": str(backup_file)}


__all__ = [
    "mock_aws",
    "lambda_matchmaker",
    "lambda_announcer",
    "lambda_historian",
]


