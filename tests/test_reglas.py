import json

import pytest

from ventas.reglas import leer_csv, revisar_calidad, separar_ventas, validar_fila

FECHA = "2026-10-05"


def fila(**cambios):
    """Fila válida de ejemplo; pásale solo lo que quieras cambiar."""
    base = {
        "id_venta": "70001",
        "fecha": FECHA,
        "region": "norte",
        "producto": "Mouse",
        "cantidad": "3",
        "precio_unitario": "45.50",
    }
    base.update(cambios)
    return base


# Estas cuatro pruebas son de ejemplo. Van a fallar hasta que completes reglas.py.

def test_fila_valida_calcula_el_total():
    registro, motivo = validar_fila(fila(), FECHA)
    assert motivo is None
    assert registro["total"] == 136.5


def test_region_desconocida_se_rechaza():
    registro, motivo = validar_fila(fila(region="Lima"), FECHA)
    assert registro is None
    assert motivo == "region_invalida"


def test_duplicado_conserva_la_primera_aparicion():
    limpias, rechazadas = separar_ventas([fila(), fila(cantidad="9")], FECHA)
    assert len(limpias) == 1
    assert limpias[0]["cantidad"] == 3
    assert rechazadas[0]["motivo"] == "id_duplicado"


def test_calidad_rechaza_sobre_el_umbral():
    with pytest.raises(ValueError):
        revisar_calidad(10, 3)


# --- Normalización de filas válidas ---

@pytest.mark.parametrize("region, esperada", [
    ("  SUR ", "Sur"),
    ("norte", "Norte"),
    ("Centro ", "Centro"),
])
def test_region_se_normaliza(region, esperada):
    registro, motivo = validar_fila(fila(region=region), FECHA)
    assert motivo is None
    assert registro["region"] == esperada


@pytest.mark.parametrize("producto, esperado", [
    (" AURICULARES", "Auriculares"),
    ("mouse", "Mouse"),
    ("MOUSE INALÁMBRICO", "Mouse inalámbrico"),
])
def test_producto_se_normaliza(producto, esperado):
    registro, motivo = validar_fila(fila(producto=producto), FECHA)
    assert motivo is None
    assert registro["producto"] == esperado


def test_registro_valido_tiene_tipos_y_redondeo():
    registro, _ = validar_fila(fila(id_venta=" 070001 ", cantidad="3", precio_unitario="1.256"), FECHA)
    assert registro["id_venta"] == 70001
    assert registro["cantidad"] == 3
    assert registro["precio_unitario"] == 1.26
    assert registro["total"] == 3.78
    assert registro["fecha"] == FECHA


# --- Un caso por cada motivo de rechazo ---

@pytest.mark.parametrize("id_venta", ["", "abc", "0", "000", "-5", "+7", "70.1", "1e3", "٣"])
def test_id_invalido(id_venta):
    assert validar_fila(fila(id_venta=id_venta), FECHA) == (None, "id_invalido")


@pytest.mark.parametrize("fecha", ["2026-10-06", "", "05/10/2026"])
def test_fecha_distinta(fecha):
    assert validar_fila(fila(fecha=fecha), FECHA) == (None, "fecha_distinta")


@pytest.mark.parametrize("region", ["Lima", "Oeste", "", "nor te"])
def test_region_invalida(region):
    assert validar_fila(fila(region=region), FECHA) == (None, "region_invalida")


@pytest.mark.parametrize("producto", ["", "   "])
def test_producto_vacio(producto):
    assert validar_fila(fila(producto=producto), FECHA) == (None, "producto_vacio")


@pytest.mark.parametrize("cantidad", ["", "0", "-2", "dos", "1.5", "+3", "1_000"])
def test_cantidad_invalida(cantidad):
    assert validar_fila(fila(cantidad=cantidad), FECHA) == (None, "cantidad_invalida")


@pytest.mark.parametrize("precio", ["", "0", "0.00", "0.004", "45,50", "1e3", "5.", ".5", "nan", "-45.50"])
def test_precio_invalido(precio):
    assert validar_fila(fila(precio_unitario=precio), FECHA) == (None, "precio_invalido")


@pytest.mark.parametrize("precio, esperado", [("699", 699.0), ("45.50", 45.5), ("0.009", 0.01)])
def test_precio_valido(precio, esperado):
    registro, motivo = validar_fila(fila(precio_unitario=precio), FECHA)
    assert motivo is None
    assert registro["precio_unitario"] == esperado


# --- Columnas incorrectas leyendo un CSV real ---

def test_precio_con_coma_sin_comillas_parte_la_linea(tmp_path):
    archivo = tmp_path / "ventas.csv"
    archivo.write_text(
        "id_venta,fecha,region,producto,cantidad,precio_unitario\n"
        f"70001,{FECHA},norte,Mouse,3,45,50\n"
        f'70002,{FECHA},norte,Mouse,3,"45,50"\n',
        encoding="utf-8",
    )
    filas = leer_csv(archivo)
    assert validar_fila(filas[0], FECHA) == (None, "columnas_incorrectas")
    # con comillas la línea sí tiene seis columnas; falla después, por el precio
    assert validar_fila(filas[1], FECHA) == (None, "precio_invalido")


def test_linea_con_columnas_de_menos(tmp_path):
    archivo = tmp_path / "ventas.csv"
    archivo.write_text(
        "id_venta,fecha,region,producto,cantidad,precio_unitario\n"
        f"70001,{FECHA},norte,Mouse\n",
        encoding="utf-8",
    )
    filas = leer_csv(archivo)
    assert validar_fila(filas[0], FECHA) == (None, "columnas_incorrectas")


# --- Duplicados ---

def test_duplicado_de_una_fila_rechazada_por_otro_motivo():
    limpias, rechazadas = separar_ventas([fila(region="Lima"), fila()], FECHA)
    assert limpias == []
    assert [r["motivo"] for r in rechazadas] == ["region_invalida", "id_duplicado"]


def test_ids_se_comparan_como_numero():
    limpias, rechazadas = separar_ventas([fila(id_venta="70001"), fila(id_venta="070001")], FECHA)
    assert len(limpias) == 1
    assert rechazadas[0]["motivo"] == "id_duplicado"
    assert rechazadas[0]["id_venta"] == "070001"


def test_duplicado_no_pisa_columnas_incorrectas_ni_id_invalido():
    filas = [fila(), fila(_sobrantes=["50"]), fila(id_venta="-70001")]
    _, rechazadas = separar_ventas(filas, FECHA)
    assert [r["motivo"] for r in rechazadas] == ["columnas_incorrectas", "id_invalido"]


def test_duplicado_gana_a_las_reglas_posteriores():
    _, rechazadas = separar_ventas([fila(), fila(cantidad="0")], FECHA)
    assert rechazadas[0]["motivo"] == "id_duplicado"


# --- Orden de las reglas ---

def test_solo_se_reporta_el_primer_problema():
    mala = fila(fecha="2026-10-06", region="Lima", producto="", cantidad="0", precio_unitario="0")
    assert validar_fila(mala, FECHA) == (None, "fecha_distinta")
    mala = fila(region="Lima", producto="", cantidad="0")
    assert validar_fila(mala, FECHA) == (None, "region_invalida")
    mala = fila(cantidad="dos", precio_unitario="45,50")
    assert validar_fila(mala, FECHA) == (None, "cantidad_invalida")


# --- separar_ventas ---

def test_separar_ventas_no_pierde_filas():
    filas = [
        fila(id_venta="1"),
        fila(id_venta="2", region="Lima"),
        fila(id_venta="1"),
        fila(id_venta=""),
        fila(id_venta="3", precio_unitario="45,50"),
        fila(id_venta="4"),
    ]
    limpias, rechazadas = separar_ventas(filas, FECHA)
    assert len(limpias) + len(rechazadas) == len(filas)
    assert [r["id_venta"] for r in limpias] == [1, 4]


def test_rechazada_guarda_linea_id_y_fila_original():
    original = fila(id_venta="", producto="Mouse inalámbrico")
    _, rechazadas = separar_ventas([fila(), original], FECHA)
    rechazada = rechazadas[0]
    assert rechazada["linea"] == 3  # la línea 1 es el encabezado
    assert rechazada["id_venta"] is None
    assert rechazada["motivo"] == "id_invalido"
    assert json.loads(rechazada["fila_original"]) == original
    assert "inalámbrico" in rechazada["fila_original"]


# --- Control de calidad ---

def test_calidad_acepta_la_tasa_exacta_del_umbral():
    assert revisar_calidad(10, 2) == pytest.approx(0.20)


def test_calidad_acepta_sin_rechazos():
    assert revisar_calidad(46, 0) == 0


def test_calidad_falla_con_archivo_vacio():
    with pytest.raises(ValueError):
        revisar_calidad(0, 0)


def test_calidad_usa_el_umbral_recibido():
    with pytest.raises(ValueError):
        revisar_calidad(10, 2, umbral=0.10)
