# ============================================================
# serve_web.py — Servidor HTTP local para desarrollo
#
# Sirve el frontend estático desde docs/ en http://localhost:8000
# Equivalente a lo que hace GitHub Pages en producción.
#
# Uso: python scripts/serve_web.py
# ============================================================

import http.server
import socketserver
import os

PORT = 8000
DIRECTORY = "docs"

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # Apuntar la raíz del servidor a docs/ en lugar del directorio de trabajo
        super().__init__(*args, directory=DIRECTORY, **kwargs)

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    print(f"--- Portal de Transparencia: Rincón de la Victoria ---")
    print(f"Servidor iniciado en http://localhost:{PORT}")
    print("Presiona Ctrl+C para detener el servidor.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServidor detenido.")
