import json

from flask import Response


def json_response(status=200, obj={}, sort=True):
    # wrap a response
    r = {"_system": "geekree api system", "result": obj}
    return Response(
        response = json.dumps(r, ensure_ascii=False, sort_keys=sort, indent=2),
        status = 200,
        mimetype="application/json")