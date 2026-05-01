# BestCashOps Context

Documento vivo con el contexto operativo del proyecto BestCashOps. La idea es usarlo como referencia rapida cada vez que trabajemos sobre este repo o sobre la infraestructura asociada.

## Proyecto

- Repo local: `/Users/JoseManuel.Gilperez/Documents/GitHub/BestCashOps`
- Repo remoto observado en VPS B: `/root/BestCashOps`
- Objetivo del repo:
  - automatizaciones Python para retail
  - pipeline mayorista
  - utilidades operativas de media, exportacion, mantenimiento e impresion
- El frontend de tienda `bestcash` no vive aqui; este repo se centra en procesos de datos, stock y publicacion.

## Infraestructura conocida

### Servidor A

- IP publica: `212.227.110.239`
- Hostname observado: `ubuntu`
- Acceso esperado: `root@212.227.110.239`
- SO: Ubuntu 24.04.4 LTS
- Kernel: `6.8.0-110-generic`
- Virtualizacion: KVM
- Recursos:
  - 4 vCPU
  - 7.7 GiB RAM
  - 240 GB disco
- Puertos expuestos:
  - `22` SSH
  - `80` Nginx
  - `443` Nginx
  - `5678` `n8n` via Docker
  - `3000` servicio Playwright via Docker
- Firewall:
  - `ufw` instalado pero inactivo
  - reglas Docker activas en `iptables`
- Web stack:
  - Nginx instalado
  - site habilitado: `n8n`
  - Certbot instalado
- Docker:
  - Engine 29.4.1
  - contenedores:
    - `ai-stack-n8n-1`
    - `playwright-service`
  - compose file: `/root/ai-stack/docker-compose.yml`
- Directorios relevantes:
  - `/root/ai-stack`
  - `/opt/n8n_files`
  - `/root/run_amazon_pipeline.sh`
- Bases de datos observadas:
  - no se detectan MySQL, Postgres, Redis o MongoDB locales
- Lectura funcional actual:
  - servidor dedicado fundamentalmente a `n8n`
  - aqui se automatizan procesos
  - `playwright-service` existe pero de momento no se esta usando en la operativa

### Servidor B

- IP publica: `212.227.90.202`
- Acceso confirmado en tu nota: `ssh root@212.227.90.202`
- Hostname observado: `ubuntu`
- SO: Ubuntu 24.04.4 LTS
- Kernel: `6.8.0-31-generic`
- Virtualizacion: Microsoft
- Recursos:
  - 2 vCPU
  - 3.8 GiB RAM
  - 120 GB disco
- Puertos expuestos:
  - `22` SSH
- Firewall:
  - `ufw` instalado pero inactivo
- Docker/Nginx:
  - no se detecta Docker en uso
  - Nginx no esta instalado
- Directorios relevantes:
  - `/root/BestCashOps`
  - `/opt/bestcash`
  - `/shops`
  - `/tpv`
  - `/wallapop`
- Scripts relevantes en `/root`:
  - `run_boxes_pipeline.sh`
  - `run_import_pipeline.sh`
  - `run_shopify_sync.sh`
  - `run_status_and_deploy.sh -> /root/BestCashOps/run_status_and_deploy.sh`
- Cron detectado:
  - `0 3 * * * /root/run_shopify_sync.sh`
- SSH:
  - login como `root` habilitado
  - varias claves publicas autorizadas en `/root/.ssh/authorized_keys`
- Bases de datos observadas:
  - no se detectan MySQL, Postgres, Redis o MongoDB locales
- Lectura funcional actual:
  - este es el servidor donde esta clonado `BestCashOps`
  - aqui se ejecutan los scripts invocados desde `n8n`
  - actua como nodo de ejecucion operativa de los procesos automatizados

## Auditorias fuente

- Servidor A: [audits/ubuntu-2026-05-01-105518.tar.gz](/Users/JoseManuel.Gilperez/Documents/GitHub/BestCashOps/audits/ubuntu-2026-05-01-105518.tar.gz)
- Servidor B: [audits/ubuntu-2026-05-01-105556.tar.gz](/Users/JoseManuel.Gilperez/Documents/GitHub/BestCashOps/audits/ubuntu-2026-05-01-105556.tar.gz)

## Estructura del repo

- `base/amazon_import/`: importacion y sincronizacion retail
- `base/bestcash_modules/`: modulos compartidos
- `wholesale/pipeline/`: ingest, enrich y finance para lotes
- `wholesale/scripts/`: wrappers operativos
- `wholesale/web/`: generacion HTML y assets
- `tools/`: utilidades operativas varias
- `N8N_WORKFLOWS.md`: inventario y documentacion funcional de workflows de `n8n`

## Flujo operativo inferido

### Repo y ejecucion principal

- El repo de trabajo principal en servidor parece ser `/root/BestCashOps` en el Servidor B.
- Los scripts en `/root` actuan como puntos de entrada operativos para tareas recurrentes.
- `run_status_and_deploy.sh` esta enlazado al script homonimo del repo.

### Separacion de responsabilidades confirmada

- Servidor A:
  - `n8n`
  - orquestacion de automatizaciones
  - posible uso futuro de Playwright
- Servidor B:
  - clon operativo del repo `BestCashOps`
  - ejecucion real de scripts lanzados desde `n8n`
  - scripts y directorios auxiliares se documentaran mas adelante segun se vayan usando

## Convenciones de trabajo propuestas

- Tratar Servidor B como fuente principal de ejecucion operativa salvo que un flujo dependa claramente de `n8n` o Playwright.
- Tratar Servidor A como servidor de apoyo para automatizacion e integraciones.
- Considerar `n8n` como foco actual del proyecto mientras se definen o amplian automatizaciones.
- Antes de tocar despliegues o cron:
  - revisar el script wrapper correspondiente en `/root`
  - comprobar si depende de variables de entorno o archivos fuera del repo
  - validar si escribe en Shopify, FTP, S3 o servicios externos

## Riesgos u observaciones detectadas

- `PermitRootLogin yes` en ambos VPS.
- `ufw` aparece inactivo en ambos VPS.
- No se observan bases de datos locales, asi que probablemente hay dependencias externas no documentadas todavia.
- En el Servidor A, el site Nginx y `n8n` estan expuestos; conviene aclarar si estan detras de autenticacion adicional.
- En el Servidor B, la tarea diaria de Shopify a las 03:00 UTC merece documentacion funcional.

## Preguntas abiertas

Estas preguntas quedan aqui para ir cerrando el contexto:

1. En `Servidor A`, que workflows de `n8n` son los mas importantes ahora mismo?
2. En `Servidor B`, que hace exactamente cada wrapper?
   - `run_boxes_pipeline.sh`
   - `run_import_pipeline.sh`
   - `run_shopify_sync.sh`
   - `run_status_and_deploy.sh`
3. `/opt/bestcash`, `/shops`, `/tpv` y `/wallapop` que contienen y cuales son criticos para este repo?
4. Donde viven las variables sensibles y credenciales reales?
   - `.env`
   - variables exportadas en shell
   - archivos en `/root`
   - AWS credentials
5. Este repo despliega a algun servicio externo concreto ademas de Shopify, FTP, S3 y OpenAI?
6. Quieres que documentemos tambien:
   - dominios y subdominios
   - certificados
   - jobs cron
   - dependencias externas
   - procedimientos de despliegue y rollback

## Estado del documento

Ultima actualizacion inicial basada en auditorias del `2026-05-01`.
