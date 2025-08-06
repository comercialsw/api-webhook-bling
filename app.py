import os
import hmac
import hashlib
import json
from flask import Flask, request, jsonify
import psycopg2

app = Flask(__name__)

# Todas as configurações sensíveis via variáveis de ambiente
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT", "5432")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

def trata_data(data_str):
    if data_str in [None, "", "0000-00-00"]:
        return None
    return data_str

def valida_hash(raw_body, received_signature):
    if not received_signature:
        return False
    hash_own = hmac.new(CLIENT_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    return received_signature == f"sha256={hash_own}"

def upsert_pedido(cursor, pedido):
    sql = """
    INSERT INTO pedidos (
        id_bling, n_pedido, id_loja, id_nf, data_pedido, data_expedicao, data_entrega,
        valor_pedido, id_situacao, desconto, atualizado_em
    ) VALUES (
        %(id_bling)s, %(n_pedido)s, %(id_loja)s, %(id_nf)s, %(data_pedido)s, %(data_expedicao)s, %(data_entrega)s,
        %(valor_pedido)s, %(id_situacao)s, %(desconto)s, NOW()
    )
    ON CONFLICT (id_bling) DO UPDATE SET
        n_pedido = EXCLUDED.n_pedido,
        id_loja = EXCLUDED.id_loja,
        id_nf = EXCLUDED.id_nf,
        data_pedido = EXCLUDED.data_pedido,
        data_expedicao = EXCLUDED.data_expedicao,
        data_entrega = EXCLUDED.data_entrega,
        valor_pedido = EXCLUDED.valor_pedido,
        id_situacao = EXCLUDED.id_situacao,
        desconto = EXCLUDED.desconto,
        atualizado_em = NOW();
    """
    cursor.execute(sql, pedido)

def upsert_item(cursor, item):
    sql = """
    INSERT INTO itens_pedido (
        id_pedido, sku, quantidade, valor_item, produto
    ) VALUES (
        %(id_pedido)s, %(sku)s, %(quantidade)s, %(valor_item)s, %(produto)s
    )
    ON CONFLICT (id_pedido, sku) DO UPDATE SET
        quantidade = EXCLUDED.quantidade,
        valor_item = EXCLUDED.valor_item,
        produto = EXCLUDED.produto;
    """
    cursor.execute(sql, item)

def get_id_pedido(cursor, id_bling):
    cursor.execute("SELECT id FROM pedidos WHERE id_bling = %s;", (id_bling,))
    row = cursor.fetchone()
    return row[0] if row else None

def processa_pedido(detalhes, cursor):
    pedido = {
        "id_bling": detalhes.get("id"),
        "n_pedido": detalhes.get("numero"),
        "id_loja": detalhes.get("loja", {}).get("id"),
        "id_nf": detalhes.get("notaFiscal", {}).get("id"),
        "data_pedido": trata_data(detalhes.get("data")),
        "data_expedicao": trata_data(detalhes.get("dataSaida")),
        "data_entrega": trata_data(detalhes.get("dataPrevista")),
        "valor_pedido": detalhes.get("total"),
        "id_situacao": str(detalhes.get("situacao", {}).get("id")),
        "desconto": detalhes.get("desconto", {}).get("valor")
    }
    upsert_pedido(cursor, pedido)
    id_pedido_interno = get_id_pedido(cursor, pedido["id_bling"])
    for item in detalhes.get("itens", []):
        item_registro = {
            "id_pedido": id_pedido_interno,
            "sku": item.get("codigo"),
            "quantidade": item.get("quantidade"),
            "valor_item": item.get("valor"),
            "produto": item.get("descricao")
        }
        upsert_item(cursor, item_registro)

@app.route('/webhook-bling', methods=['POST'])
def webhook_bling():
    raw = request.get_data()
    signature = request.headers.get("X-Bling-Signature-256", "")
    if not valida_hash(raw, signature):
        return jsonify({"error": "Invalid signature"}), 401
    try:
        payload = json.loads(raw)
        event_type = payload.get("event", "")
        # Só processa eventos de pedidos
        if event_type.startswith("order."):
            detalhes = payload.get("data", {})
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                port=DB_PORT
            )
            cur = conn.cursor()
            processa_pedido(detalhes, cur)
            conn.commit()
            cur.close()
            conn.close()
            return jsonify({"status": "ok"}), 200
        else:
            print(f"Evento ignorado: {event_type}")
            return jsonify({"status": f"Evento ignorado: {event_type}"}), 200
    except Exception as e:
        print("Erro ao processar pedido:", e)
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000)
