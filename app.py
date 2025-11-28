from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from colorama import Fore, Style, init
from flask import Flask, jsonify, request
from tinydb import TinyDB, Query

from gateway import api_gateway
from lambdas import (
    lambda_historian,
    lambda_matchmaker,
    lambda_notifier,
    lambda_scheduler,
    lambda_statistics,
    lambda_validator,
    mock_aws,
)

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
    """Insere um atleta no TinyDB após validação via Lambda Validator."""
    if not request.is_json:
        return _json_response({"erro": "Envie JSON válido."}, 400)

    payload = request.get_json()
    required = ["nome", "faixa", "categoria"]
    missing = [field for field in required if not payload.get(field)]
    if missing:
        return _json_response({"erro": f"Campos obrigatórios: {', '.join(missing)}"}, 400)

    atleta_data = {
        "nome": payload["nome"],
        "faixa": payload["faixa"],
        "categoria": payload["categoria"],
        "equipe": payload.get("equipe", "Independente"),
    }

    # Valida via Lambda Validator
    validation_result = mock_aws.invoke_lambda("Lambda Validator", lambda_validator, {"atleta": atleta_data})
    if not validation_result.get("valido"):
        return _json_response({"erro": "Validação falhou.", "detalhes": validation_result.get("erros", [])}, 400)

    # Insere com ID único
    atleta_id = atletas_table.insert(atleta_data)
    atleta_data["id"] = atleta_id
    atletas_table.update({"id": atleta_id}, doc_ids=[atleta_id])

    print(Fore.CYAN + f"[API] Atleta cadastrado: {atleta_data['nome']} (ID: {atleta_id})")
    return _json_response({"mensagem": "Atleta cadastrado com sucesso.", "atleta": atleta_data}, 201)


@app.post("/gerar-chaves")
def gerar_chaves():
    """Invoca a Lambda Matchmaker para gerar os confrontos e Lambda Scheduler para agendar."""
    atletas = atletas_table.all()
    if len(atletas) < 2:
        return _json_response({"erro": "Cadastre pelo menos dois atletas antes."}, 400)

    # Gera chaves via Lambda Matchmaker
    event = {"atletas": atletas}
    resultado = mock_aws.invoke_lambda("Lambda 1 - Matchmaker", lambda_matchmaker, event)
    confrontos: List[Dict[str, Any]] = resultado.get("confrontos", [])

    if not confrontos:
        return _json_response({"erro": "Não foi possível gerar confrontos."}, 400)

    # Agenda lutas via Lambda Scheduler
    schedule_result = mock_aws.invoke_lambda("Lambda Scheduler", lambda_scheduler, {"chaves": confrontos})
    lutas_agendadas = schedule_result.get("lutas_agendadas", [])

    # Salva no TinyDB com IDs
    chaves_table.truncate()
    ids_inseridos = chaves_table.insert_multiple(confrontos)
    for idx, chave_id in enumerate(ids_inseridos):
        chaves_table.update({"id": chave_id}, doc_ids=[chave_id])

    print(Fore.BLUE + f"[API] {len(confrontos)} confrontos salvos no TinyDB.")

    return _json_response(
        {
            "mensagem": "Chaves geradas e agendadas.",
            "confrontos": confrontos,
            "lutas_agendadas": lutas_agendadas,
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
    """Armazena o vencedor, dispara backup via Lambda Historian e notifica via Lambda Notifier."""
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
    resultado_id = resultados_table.insert(registro)
    registro["id"] = resultado_id
    resultados_table.update({"id": resultado_id}, doc_ids=[resultado_id])
    print(Fore.LIGHTGREEN_EX + f"[API] Resultado salvo para {registro['luta_id']} (ID: {resultado_id}).")

    # Backup via Lambda Historian
    backup_payload = {
        "luta_id": registro["luta_id"],
        "vencedor": registro["vencedor"],
        "submitido_por": registro["metodo"],
        "extra": {"tempo": registro["tempo"]},
    }
    backup_result = mock_aws.invoke_lambda("Lambda 3 - Historian", lambda_historian, backup_payload)

    # Notificação via Lambda Notifier
    notify_payload = {
        "luta_id": registro["luta_id"],
        "vencedor": registro["vencedor"],
        "metodo": registro["metodo"],
    }
    notify_result = mock_aws.invoke_lambda("Lambda Notifier", lambda_notifier, notify_payload)

    return _json_response(
        {
            "mensagem": "Resultado registrado, backup realizado e notificação enviada.",
            "resultado": registro,
            "backup": backup_result,
            "notificacao": notify_result,
        },
        201,
    )


@app.get("/atletas")
def listar_atletas():
    """Lista todos os atletas cadastrados."""
    atletas = atletas_table.all()
    return _json_response({"total": len(atletas), "atletas": atletas})


@app.get("/atletas/<int:atleta_id>")
def buscar_atleta(atleta_id: int):
    """Busca um atleta específico por ID."""
    atleta = atletas_table.get(doc_id=atleta_id)
    if not atleta:
        return _json_response({"erro": f"Atleta com ID {atleta_id} não encontrado."}, 404)
    return _json_response({"atleta": atleta})


@app.get("/chaves")
def listar_chaves():
    """Lista todas as chaves geradas."""
    chaves = chaves_table.all()
    return _json_response({"total": len(chaves), "chaves": chaves})


@app.get("/resultados")
def listar_resultados():
    """Lista todos os resultados registrados."""
    resultados = resultados_table.all()
    return _json_response({"total": len(resultados), "resultados": resultados})


@app.get("/resultados/<luta_id>")
def buscar_resultado(luta_id: str):
    """Busca resultado de uma luta específica."""
    Resultado = Query()
    resultado = resultados_table.search(Resultado.luta_id == luta_id)
    if not resultado:
        return _json_response({"erro": f"Resultado para luta {luta_id} não encontrado."}, 404)
    return _json_response({"resultado": resultado[0] if len(resultado) == 1 else resultado})


@app.get("/estatisticas")
def obter_estatisticas():
    """Calcula e retorna estatísticas do torneio via Lambda Statistics."""
    atletas = atletas_table.all()
    resultados = resultados_table.all()

    stats_result = mock_aws.invoke_lambda(
        "Lambda Statistics", lambda_statistics, {"atletas": atletas, "resultados": resultados}
    )

    return _json_response({"estatisticas": stats_result})


@app.get("/status")
def status_sistema():
    """Retorna status do sistema e integrações."""
    gateway_stats = api_gateway.get_stats()
    total_atletas = len(atletas_table.all())
    total_chaves = len(chaves_table.all())
    total_resultados = len(resultados_table.all())

    return _json_response(
        {
            "status": "operacional",
            "banco_dados": {
                "atletas": total_atletas,
                "chaves": total_chaves,
                "resultados": total_resultados,
            },
            "gateway": gateway_stats,
            "integracao_aws": {
                "sqs": "simulado",
                "sns": "simulado",
                "lambda": "simulado",
            },
        }
    )


@app.delete("/limpar")
def limpar_dados():
    """Limpa todos os dados do banco (útil para testes)."""
    atletas_table.truncate()
    chaves_table.truncate()
    resultados_table.truncate()
    print(Fore.YELLOW + "[API] Todos os dados foram limpos.")
    return _json_response({"mensagem": "Todos os dados foram limpos com sucesso."})


@app.get("/")
def raiz():
    """Mensagem rápida para indicar que a API está viva."""
    return _json_response(
        {
            "status": "ok",
            "sistema": "Torneio de Jiu-Jitsu",
            "rotas": {
                "POST": [
                    "/atletas - Cadastrar atleta",
                    "/gerar-chaves - Gerar chaves do torneio",
                    "/chamar-luta - Enfileirar luta",
                    "/resultado - Registrar resultado",
                ],
                "GET": [
                    "/atletas - Listar atletas",
                    "/atletas/<id> - Buscar atleta",
                    "/chaves - Listar chaves",
                    "/resultados - Listar resultados",
                    "/resultados/<luta_id> - Buscar resultado",
                    "/estatisticas - Estatísticas do torneio",
                    "/status - Status do sistema",
                ],
                "DELETE": ["/limpar - Limpar todos os dados"],
            },
        }
    )


if __name__ == "__main__":
    # Executar com: python app.py
    print(Fore.WHITE + Style.BRIGHT + "[API] Iniciando servidor Flask em http://127.0.0.1:5000")
    app.run(debug=True)


