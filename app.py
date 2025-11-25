from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from colorama import Fore, Style, init
from flask import Flask, jsonify, request
from tinydb import TinyDB

from lambdas import lambda_historian, lambda_matchmaker, mock_aws

# Inicia colorama para manter o padrão de logs coloridos
init(autoreset=True)

app = Flask(__name__)

DB_PATH = Path("db.json")
db = TinyDB(DB_PATH)
atletas_table = db.table("atletas")
chaves_table = db.table("chaves")
resultados_table = db.table("resultados")

QUEUE_NAME = "lutas"


def _json_response(data: Dict[str, Any], status: int = 200):
    return jsonify(data), status


@app.post("/atletas")
def cadastrar_atleta():
    """Insere um atleta no TinyDB."""
    if not request.is_json:
        return _json_response({"erro": "Envie JSON válido."}, 400)

    payload = request.get_json()
    required = ["nome", "faixa", "categoria"]
    missing = [field for field in required if not payload.get(field)]
    if missing:
        return _json_response({"erro": f"Campos obrigatórios: {', '.join(missing)}"}, 400)

    atleta = {
        "nome": payload["nome"],
        "faixa": payload["faixa"],
        "categoria": payload["categoria"],
        "equipe": payload.get("equipe", "Independente"),
    }
    atletas_table.insert(atleta)
    print(Fore.CYAN + f"[API] Atleta cadastrado: {atleta['nome']}")
    return _json_response({"mensagem": "Atleta cadastrado com sucesso.", "atleta": atleta}, 201)


@app.post("/gerar-chaves")
def gerar_chaves():
    """Invoca a Lambda Matchmaker para gerar os confrontos."""
    atletas = atletas_table.all()
    if len(atletas) < 2:
        return _json_response({"erro": "Cadastre pelo menos dois atletas antes."}, 400)

    event = {"atletas": atletas}
    resultado = mock_aws.invoke_lambda("Lambda 1 - Matchmaker", lambda_matchmaker, event)
    confrontos: List[Dict[str, Any]] = resultado.get("confrontos", [])

    if not confrontos:
        return _json_response({"erro": "Não foi possível gerar confrontos."}, 400)

    chaves_table.truncate()
    chaves_table.insert_multiple(confrontos)
    print(Fore.BLUE + f"[API] {len(confrontos)} confrontos salvos no TinyDB.")

    return _json_response(
        {
            "mensagem": "Chaves geradas.",
            "confrontos": confrontos,
            "gerado_em": resultado.get("gerado_em"),
        }
    )


@app.post("/chamar-luta")
def chamar_luta():
    """Simula publicação de mensagem na fila SQS."""
    if not request.is_json:
        return _json_response({"erro": "Envie JSON válido."}, 400)

    payload = request.get_json()
    required = ["luta_id", "atletas"]
    missing = [field for field in required if not payload.get(field)]
    if missing:
        return _json_response({"erro": f"Campos obrigatórios: {', '.join(missing)}"}, 400)

    mensagem = {
        "luta_id": payload["luta_id"],
        "atletas": payload["atletas"],
        "round": payload.get("round", "Classificatórias"),
        "tatame": payload.get("tatame", "Principal"),
    }
    mock_aws.send_sqs(QUEUE_NAME, mensagem)
    return _json_response({"mensagem": "Luta enfileirada com sucesso.", "payload": mensagem}, 202)


@app.post("/resultado")
def registrar_resultado():
    """Armazena o vencedor e dispara backup via Lambda Historian."""
    if not request.is_json:
        return _json_response({"erro": "Envie JSON válido."}, 400)

    payload = request.get_json()
    required = ["luta_id", "vencedor"]
    missing = [field for field in required if not payload.get(field)]
    if missing:
        return _json_response({"erro": f"Campos obrigatórios: {', '.join(missing)}"}, 400)

    registro = {
        "luta_id": payload["luta_id"],
        "vencedor": payload["vencedor"],
        "metodo": payload.get("metodo", "Pontos"),
        "tempo": payload.get("tempo", "00:00"),
        "registrado_em": datetime.utcnow().isoformat() + "Z",
    }
    resultados_table.insert(registro)
    print(Fore.LIGHTGREEN_EX + f"[API] Resultado salvo para {registro['luta_id']}.")

    backup_payload = {
        "luta_id": registro["luta_id"],
        "vencedor": registro["vencedor"],
        "submitido_por": registro["metodo"],
        "extra": {"tempo": registro["tempo"]},
    }
    backup_result = mock_aws.invoke_lambda("Lambda 3 - Historian", lambda_historian, backup_payload)

    return _json_response(
        {
            "mensagem": "Resultado registrado e backup realizado.",
            "backup": backup_result,
        },
        201,
    )


@app.get("/")
def raiz():
    """Mensagem rápida para indicar que a API está viva."""
    return _json_response(
        {
            "status": "ok",
            "rotas": [
                "POST /atletas",
                "POST /gerar-chaves",
                "POST /chamar-luta",
                "POST /resultado",
            ],
        }
    )


if __name__ == "__main__":
    # Executar com: python app.py
    print(Fore.WHITE + Style.BRIGHT + "[API] Iniciando servidor Flask em http://127.0.0.1:5000")
    app.run(debug=True)


