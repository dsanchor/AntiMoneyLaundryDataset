# Changelog

Todos los cambios relevantes de datos, esquemas y automatizacion de este repositorio se
documentan aqui. El formato sigue [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [2026-08-18]

### Added

- Se agrego la columna `country` a la tabla Delta `dim_bank` del Lakehouse `GOLD`.
- Se agrego `src/enrich_bank_country.py` para reproducir el enriquecimiento en Fabric.
- Se agrego una validacion al exportador que impide publicar `dim_bank` sin `country`.

### Changed

- Se reemplazo el snapshot Git LFS de `dim_bank` por un Parquet/Zstandard con las
  columnas `bank_id`, `bank_name` y `country`.
- Se generalizo la guia de despliegue para que no contenga nombres personales ni IDs de
  un entorno concreto.

### Data Methodology

La asignacion de pais usa la siguiente prioridad:

1. Coincidencia exacta y no ambigua de `bank_name` con bancos de Wikidata que tienen
   propiedad de pais.
2. Prefijo del patron sintetico `<pais> Bank #<numero>`, contrastado con el catalogo de
   paises del Banco Mundial. Se normalizan aliases como `UK` a `United Kingdom`.
3. Los nombres `Crytpo Bank #<numero>` se asignan a `Unknown` porque no contienen una
   jurisdiccion inferible.
4. Los nombres sinteticos residuales se distribuyen de forma determinista mediante
   `pmod(xxhash64(bank_id), 4)` entre `United States`, `Nigeria`, `Bangladesh` y
   `Panama`. Esta distribucion es sintetica y no representa una afirmacion sobre una
   entidad bancaria real.

Fuentes de referencia:

- [Wikidata Query Service](https://query.wikidata.org/), clase banco y propiedad pais.
- [World Bank Country API](https://api.worldbank.org/v2/country?format=json&per_page=400),
  utilizada para validar nombres y codigos de pais.

### Validation

- Filas antes y despues: `122,333`.
- Paises distintos: `37`.
- Valores nulos en `country`: `0`.
- Version Delta de `dim_bank`: `0` antes del primer enriquecimiento y `2` despues de
  incorporar la distribucion residual.
- Conteos solicitados: `Mexico=3,608`, `Nigeria=10,598`, `Bangladesh=10,528` y
  `Panama=10,628`.

## [2026-08-17]

### Added

- Snapshot Gold Parquet/Zstandard gestionado mediante Git LFS.
- Workflow GitHub Actions con autenticacion OIDC para desplegar `GOLD` en Fabric.
- Aprovisionamiento idempotente, carga OneLake, materializacion Delta y validaciones.
- Guia para desplegar el modelo en un entorno Fabric propio.

### Validation

- Despliegue validado con ocho tablas y `31,898,238` filas en `fact_transaction`.
- Tiempo observado del workflow completo en capacidad F64: aproximadamente 2 minutos
  y 46 segundos.