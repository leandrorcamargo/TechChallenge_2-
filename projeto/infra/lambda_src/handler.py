"""Placeholder da Lambda produtora do streaming.

A implementação real (ler o delta do arquivo raw recém-chegado e publicar os
registros no Kinesis via PutRecords) é entregue na etapa C8. Este stub existe para
que o `terraform apply` consiga empacotar e criar a função desde já.
"""

import json
import os


def lambda_handler(event, context):
    """Recebe o evento do EventBridge (S3 ObjectCreated) e registra em log.

    Na etapa C8 este corpo passará a ler o objeto do S3 e publicar no Kinesis.
    """
    stream = os.environ.get("KINESIS_STREAM", "<nao-configurado>")
    print(json.dumps({
        "msg": "producer placeholder acionado",
        "kinesis_stream": stream,
        "event": event,
    }))
    return {"status": "placeholder", "stream": stream}
