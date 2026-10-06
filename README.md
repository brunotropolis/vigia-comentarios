# vigia-comentarios

Serviço do Motor de Conteúdo (Manual do Recém-Nascido). A cada `INTERVALO_S` segundos consulta a contagem de
comentários dos posts vigiados (Instagram + Facebook) e, quando muda, repassa os comentários novos com
palavra-chave pro Disparador (n8n), no mesmo formato do webhook da Meta.

Env: `DATABASE_URL` (obrigatório), `DISPARADOR_URL`, `INTERVALO_S` (10), `JANELA_RESPOSTA_H` (24),
`QUENTE_DIAS` (60), `PAUSADO` (true/false), `IG_ID`, `FB_PAGE_ID`. Nenhum segredo no código.
`GET /` devolve o estado (ciclos, repassados, erros).
