# 🧠 Data Science Projects – UNIVPM

Questa repository raccoglie diversi progetti di Data Science sviluppati durante il corso magistrale di Ingegneria Informatica e dell'Automazione presso UNIVPM.

I progetti includono:
- Analisi dati e Machine Learning
- Natural Language Processing
- Social Network Analysis
- Un Chatbot basato su Rasa

---

## 📂 Struttura della Repository

| Cartella | Contenuto | Tecnologie principali |
|---------|-----------|-------------------|
| **Python** | Classificazione/Regressione/Clusterizzazione dei vini + Analisi e previsioni temporali del prezzo E.A.| scikit-learn statsmodels |
| **SNA** | Analisi rete Spotify | NetworkX |
| **NLP** | Sentiment analysis commenti Reddit | Transformers PyTorch BERT |
| **Chatbot** | Bot conversazionale ATP single | RASA framework |
| **NLP 2** | Text Classification umano vs ai + NER | TF-IDF, Word2Vec, spaCy |

---

## 🤖 Chatbot Rasa – Avvio del progetto

> Requisiti: Python 3.10, Rasa >= 3.0, virtualenv o Conda

### 🔧 Creazione e attivazione ambiente virtuale

```bash
cd Chatbot
python -m venv venv
venv\Scripts\activate   # Windows
```

### 📦 Installazione dipendenze

```bash
pip install -r requirements.txt
```

### 🗄️ Creazione del database locale

```bash
python db_create.py
```
Verrà creato il file tennisbot.db con giocatori e storico partite.

### 🧠 Addestramento del modello Rasa

```bash
rasa train
```

🧪 Test in locale da terminale

🔹 Aprire 2 terminali:

📌 Terminale 1 → Action Server

```bash
cd Chatbot
venv\Scripts\activate
rasa run actions
```

📌 Terminale 2 → Conversazione

```bash
cd Chatbot
venv\Scripts\activate
rasa shell
```

🌍 Telegram + ngrok (solo Windows)

Per esporre il bot pubblicamente:

```bash
cd Chatbot
./start_bot.ps1
```

⚠️ Configurazione necessaria prima dell’avvio

📌 In credentials.yml

```bash
telegram:
  access_token: "BOT_TOKEN_DA_BOTFATHER"
  verify: "NOME_BOT"
  webhook_url: "https://<NGROK_ID>.ngrok.io/webhooks/telegram/webhook"
```

📌 In endpoints.yml
Assicurarsi che il canale Telegram sia abilitato.
