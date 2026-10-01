# Raft: шаг 1

Только выбор лидера, heartbeat и REST-статус по статье Ongaro / Ousterhout,
§5.1–5.2 и Figure 2. Репликации журнала, key-value и распределённых блокировок здесь нет.

## Установка

Нужен Python 3.11+. Распакуйте проект и в его каталоге выполните один раз:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

## Запуск в трёх терминалах

В каждом терминале перейдите в **один и тот же каталог проекта**.

**Терминал 1**
```bash
.venv/bin/python app.py --id 1
```

**Терминал 2**
```bash
.venv/bin/python app.py --id 2
```

**Терминал 3**
```bash
.venv/bin/python app.py --id 3
```

## Статус и перезапуски

Откройте `http://127.0.0.1:8001/status` в браузере - также для портов 8002 и 8003.
