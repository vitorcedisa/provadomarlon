import time
from typing import Optional

from colorama import Fore, Style, init

from lambdas import lambda_announcer, mock_aws

# Configura Colorama para logs bonitos no Windows/macOS/Linux
init(autoreset=True)

QUEUE_NAME = "lutas"
POLL_INTERVAL = 3  # segundos


def process_next_message() -> Optional[dict]:
    """Busca uma mensagem na fila simulada e aciona a Lambda Announcer."""
    message = mock_aws.receive_sqs(QUEUE_NAME)
    if not message:
        return None

    mock_aws.invoke_lambda("Lambda 2 - Announcer", lambda_announcer, message)
    return message


def main():
    print(
        Fore.WHITE
        + Style.BRIGHT
        + "[Worker] Iniciando consumidor da fila 'lutas'. Pressione CTRL+C para sair."
    )
    while True:
        try:
            processed = process_next_message()
            if not processed:
                time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print(Fore.RED + "[Worker] Encerrado manualmente.")
            break
        except Exception as exc:  # noqa: BLE001 - log simples para demo
            print(Fore.RED + f"[Worker] Erro inesperado: {exc}")
            time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    # Executar em um terminal separado: python worker.py
    main()


