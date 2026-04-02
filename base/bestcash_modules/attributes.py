import re


def extraer_atributos(itemdesc):
    itemdesc = itemdesc.lower() if itemdesc else ""

    talla = None
    color = None

    match = re.search(r"(talla|size)?\s*(\d{2,3})", itemdesc)
    if match:
        talla = match.group(2)

    colores = ["negro", "blanco", "rojo", "azul", "verde", "gris"]
    for color_name in colores:
        if color_name in itemdesc:
            color = color_name
            break

    return {"talla": talla, "color": color}
