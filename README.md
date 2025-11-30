# 🧠 Data Science Projects – UNIVPM

Questa repository raccoglie diversi progetti di Data Science sviluppati durante il corso magistrale di Ingegneria Informatica e dell'Automazione presso UNIVPM.

I progetti includono:
- Analisi dati e Machine Learning
- Natural Language Processing
- Social Network Analysis
- Un Chatbot basato su Rasa

---

## 📂 Struttura della Repository

| Cartella | Contenuto | Tecniche principali |
|---------|-----------|-------------------|
| **Chatbot** | Bot conversazionale con Rasa | Intent recognition, NLU, dialog policies |
| **NLP** | Sentiment analysis commenti Reddit | BERT, fine-tuning, classificazione |
| **NLP 2** | Classificazione umano/bot + NER | Embedding, CRF, spaCy |
| **Python** | Modelli ML per vari task Classificazione/Regressione/clusterizzazione + Analisi e previsioni temporali| Random Forest, SVM, Regression, ARIMA, ecc ... |
| **SNA** | Analisi rete Spotify | NetworkX |

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
