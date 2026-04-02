import json
import logging
from openai import OpenAI

from .config import OPENAI_API_KEY

client = OpenAI(api_key=OPENAI_API_KEY)


def generar_ficha_wallapop(titulo, features, descripcion, itemdesc, atributos):
    prompt = f"""
Genera un anuncio optimizado para Wallapop.

FORMATO JSON:
{{
  "titulo": "",
  "descripcion": ""
}}

REGLAS:
- No inventar datos
- Título máx 10 palabras
- Incluir tipo + marca + atributos (talla/color si existen)
- Descripción con ✔️
- Frases cortas

ATRIBUTOS:
talla: {atributos.get("talla")}
color: {atributos.get("color")}

DATOS:
ItemDesc: {itemdesc}
Título: {titulo}
Features: {features}
Descripción: {descripcion}
"""

    try:
        res = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
        )
        content = res.choices[0].message.content.strip()
        content = content.replace("```json", "").replace("```", "").strip()
        if not content.startswith("{"):
            ini = content.find("{")
            fin = content.rfind("}")
            if ini != -1 and fin != -1 and fin > ini:
                content = content[ini : fin + 1]
        return json.loads(content)
    except Exception as exc:
        logging.warning("Fallo generando ficha IA para '%s': %s", titulo, exc)
        return {"titulo": "", "descripcion": ""}


def generar_contenido_completo(titulo_original, descripcion_raw, caracteristicas_raw):
    prompt = f"""
Devuelve exclusivamente un JSON valido.

Producto:
Titulo original: {titulo_original}
Descripcion original: {descripcion_raw}
Caracteristicas:
{chr(10).join(caracteristicas_raw or [])}

Genera en espanol neutro y traduce cualquier texto de entrada que no este en espanol.
No inventes datos tecnicos. Manten fidelidad a la informacion del producto.
En hashtags usa solo espanol.
Para SEO:
- seo_title maximo 60 caracteres y enfocado a venta.
- seo_description maximo 160 caracteres con llamada a la accion.
{{
  "titulo_amazon": "...",
  "titulo_breve": "...",
  "descripcion": "...",
  "caracteristicas": "...",
  "hashtags": "...",
  "descripcion_tecnica": "...",
  "vendor": "...",
  "seo_title": "...",
  "seo_description": "..."
}}
"""
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "Eres experto en redaccion ecommerce. Devuelve solo JSON valido en espanol.",
                },
                {"role": "user", "content": prompt},
            ],
        )
        return json.loads(completion.choices[0].message.content)
    except Exception as exc:
        logging.warning("Fallo generando contenido completo IA para '%s': %s", titulo_original, exc)
        return {}


def traducir_a_espanol(texto):
    if not texto:
        return texto
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Traduce al espanol neutro de ecommerce. "
                        "Devuelve solo el texto traducido, sin comillas."
                    ),
                },
                {"role": "user", "content": str(texto)},
            ],
        )
        return completion.choices[0].message.content.strip()
    except Exception as exc:
        logging.warning("Fallo traduciendo texto a espanol: %s", exc)
        return texto


def limpiar_dimensiones_y_extraer_peso_gramos(texto_fuente):
    if not texto_fuente:
        return None, None

    prompt = f"""
Devuelve exclusivamente un JSON valido con estas claves:
{{
  "dimensiones": "",
  "peso_gramos": null
}}

Entrada (texto posiblemente mixto con dimensiones y peso):
{texto_fuente}

Reglas:
- "dimensiones": conserva solo medidas del producto (por ejemplo: "30 x 20 x 15 cm").
- "peso_gramos": numero en gramos.
- Si el peso viene en kg, lb u onzas, convierte a gramos.
- Si no puede determinarse con seguridad, devuelve null en peso_gramos.
- No inventes datos.
"""
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "Eres experto extrayendo atributos tecnicos de producto. Devuelve solo JSON valido.",
                },
                {"role": "user", "content": prompt},
            ],
        )
        payload = json.loads(completion.choices[0].message.content)
        dimensiones = payload.get("dimensiones")
        peso = payload.get("peso_gramos")
        try:
            peso = float(peso) if peso is not None else None
        except (TypeError, ValueError):
            peso = None
        return dimensiones, peso
    except Exception as exc:
        logging.warning("Fallo limpiando dimensiones/peso con IA: %s", exc)
        return None, None
