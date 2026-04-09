# FastAPI Demo

Tato slozka obsahuje oddelenou ukazkovou aplikaci nad FastAPI.

Cil:

- nezasahovat do hlavni struktury `flowcore`
- ukazat "workera" srozumitelne pres web
- mit vlastni Docker start

Demo ukazuje jednoduchy stromovy pribeh:

1. scan root
2. inspect left branch
3. inspect right branch
4. search left leaf
5. search right leaf
6. choose best path
7. write story

Na webu je videt:

- stav workflow
- fronta kroku
- aktivni worker
- log udalosti
- retry na prave vetvi

## Local Run

Nejdriv nainstaluj zavislosti:

```bash
python -m pip install fastapi uvicorn
```

Pak spust:

```bash
uvicorn demo_fastapi.app:app --host 0.0.0.0 --port 9101
```

Otevri:

```text
http://localhost:9101
```

## Docker Run

```bash
docker compose -f demo_fastapi/docker-compose.yml up --build
```

Otevri:

```text
http://localhost:9101
```
