# Fundos BR — Dashboard CVM

Dashboard de performance de fundos de investimento com dados oficiais da CVM.

## Funcionalidades

- **Categorias:** Multimercado, Ações (Long Only / Long Biased), Crédito Privado
- **Métricas:** MTD, YTD, 12M, 24M — calculados sobre cotas diárias da CVM
- **Seletor de data base:** navegue por qualquer data histórica
- **Cache diário automático:** função agendada atualiza às 07h BRT todo dia
- **Somente feeders:** exclui fundos com "MASTER" no nome
- **Ordenação** por PL, MTD, YTD, 12M ou 24M

## Estrutura

```
├── public/
│   └── index.html              ← Dashboard (HTML estático)
├── netlify/functions/
│   ├── lib/
│   │   └── cvm.js              ← Utilitários compartilhados (fetch CVM, parse CSV, cálculos)
│   ├── fundos.js               ← GET /api/fundos?baseDate=YYYY-MM-DD (computação ao vivo)
│   ├── fundos-cached.js        ← GET /api/fundos-cached?date=YYYY-MM-DD (serve do cache)
│   └── daily-cache.js          ← Função agendada: roda 07h BRT, salva em Netlify Blobs
├── netlify.toml
└── package.json
```

## Deploy no Netlify

### 1. Via GitHub (recomendado)

1. Suba esta pasta para um repositório GitHub
2. No Netlify: **Add new site → Import from Git**
3. Selecione o repositório
4. Build settings:
   - Build command: `npm install` (ou deixe vazio)
   - Publish directory: `public`
5. Clique **Deploy**

### 2. Via Netlify CLI (drag & drop não funciona com Functions)

```bash
npm install -g netlify-cli
netlify login
netlify deploy --prod
```

### 3. Netlify Blobs (cache)

O cache usa **Netlify Blobs** — disponível automaticamente em todos os planos (incluindo o gratuito). Não precisa configurar nada extra.

### Ativar a função agendada (daily-cache)

Funções agendadas (`schedule`) funcionam automaticamente após o deploy — o Netlify detecta o cron `0 10 * * *` e executa todo dia às 10:00 UTC (07:00 BRT).

Para forçar uma execução manual do cache:
```
https://SEU-SITE.netlify.app/.netlify/functions/daily-cache
```

## Fonte dos dados

- **Cadastro:** `https://dados.cvm.gov.br/dados/FI/CAD/DADOS/inf_cadastral_fi.csv`
- **Cotas diárias:** `https://dados.cvm.gov.br/dados/FI/INF/DIARIO/DADOS/inf_diario_fi_YYYYMM.csv`

A CVM publica os dados com ~1-2 dias de defasagem.

## Fluxo de dados

```
Usuário abre o site
  → front-end chama /api/fundos-cached?date=YYYY-MM-DD
      → Netlify Blobs tem cache? → retorna instantaneamente
      → Cache miss? → redireciona para /api/fundos (computação ao vivo, ~30-60s)

Todo dia às 07h BRT
  → daily-cache.js roda
  → Baixa cadastro + 26 meses de cotas da CVM
  → Calcula retornos para todos os fundos
  → Salva em Netlify Blobs como "latest" e "snapshot-YYYY-MM-DD"
```
