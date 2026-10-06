// vigia-comentarios — Motor de Conteúdo (Manual do Recém-Nascido)
// Substitui o "aviso automático de comentário" da Meta (que exige App Review) por consulta:
// a cada INTERVALO_S segundos pergunta a contagem de comentários dos posts vigiados (até 50 por pedido);
// quando a contagem muda, lê os comentários novos e repassa pro Disparador (n8n) no formato do webhook da Meta.
// Lista de vigiados = últimos 30 posts + posts antigos "quentes" (receberam comentário recentemente).
// Tudo por env (nada de segredo no código). Tabelas: conteudo.vigia_posts / conteudo.vigia_comentarios.
const http = require('http');
const { Client } = require('pg');

const ENV = process.env;
const DB = ENV.DATABASE_URL;
const DISPARADOR = ENV.DISPARADOR_URL || 'https://n8n-n8n.xktssy.easypanel.host/webhook/motor-disparador';
const INTERVALO = Number(ENV.INTERVALO_S || 10) * 1000;
const IG_ID = ENV.IG_ID || '17841403363408251';
const FB_PAGE = ENV.FB_PAGE_ID || '1753378231566500';
const JANELA_H = Number(ENV.JANELA_RESPOSTA_H || 24);     // comentário mais velho que isso não é respondido
const QUENTE_DIAS = Number(ENV.QUENTE_DIAS || 60);        // post antigo sai da vigia após N dias sem comentário
const G = 'https://graph.facebook.com/v21.0/';
// Instagram: com o app Motor publicado, a Meta já entrega o webhook de comentário direto pro Disparador.
// Vigiar o IG aqui também faria responder em dobro → desligado por padrão (VIGIAR_IG=true só se o webhook falhar).
const VIGIAR_IG = (ENV.VIGIAR_IG || 'false').toLowerCase() === 'true';
const VIGIAR_FB = (ENV.VIGIAR_FB || 'true').toLowerCase() === 'true';

const estado = { inicio: new Date().toISOString(), ciclos: 0, repassados: 0, erros: 0, ultimoErro: null, ultimoCiclo: null, pausado: false, vigiados: 0 };
const log = (...a) => console.log(new Date().toISOString(), ...a);
const espera = ms => new Promise(s => setTimeout(s, ms));

let db;
async function conectar() {
  db = new Client({ connectionString: DB, ssl: { rejectUnauthorized: false } });
  db.on('error', e => { log('pg erro', e.message); db = null; });
  await db.connect();
}
async function q(sql, p) { if (!db) await conectar(); return (await db.query(sql, p)).rows; }

async function get(url) {
  const r = await fetch(url);
  const j = await r.json().catch(() => ({}));
  if (j.error) throw new Error(`Graph ${j.error.code}: ${j.error.message}`.slice(0, 200));
  return j;
}

// ---- tokens + gatilhos (recarrega a cada 5 min) ----
let tk = {}, gatilhos = [], carregadoEm = 0;
async function carregar() {
  if (Date.now() - carregadoEm < 5 * 60e3) return;
  const rows = await q("select rede, access_token from conteudo.tokens_social where rede in ('facebook','facebook_motor','instagram')");
  const t = Object.fromEntries(rows.map(r => [r.rede, r.access_token]));
  const sis = t.facebook_motor || t.facebook;
  const pg = await get(`${G}${FB_PAGE}?fields=access_token&access_token=${sis}`);
  tk = { leitorIG: t.facebook, pagina: pg.access_token, ig: t.instagram };
  gatilhos = [...new Set((await q("select gatilhos from conteudo.automacoes_social where ativo")).flatMap(r => r.gatilhos || []).map(norm))].filter(Boolean);
  carregadoEm = Date.now();
}
const norm = s => String(s || '').toLowerCase().replace(/[#@]/g, '').trim();
const temGatilho = t => { const p = norm(t).split(/[^a-z0-9à-ÿ]+/i).filter(Boolean); return gatilhos.some(g => p.includes(g)); };

// ---- lista de vigiados ----
async function vigiados(rede) {
  return q("select post_id, ultimo_count from conteudo.vigia_posts where rede=$1 and ativo", [rede]);
}
const jaGravados = new Set(); // evita regravar os mesmos posts recentes a cada ciclo
async function upsertPost(rede, id, motivo, publicado, legenda) {
  if (jaGravados.has(rede + id)) return;
  jaGravados.add(rede + id);
  await q(`insert into conteudo.vigia_posts (rede, post_id, motivo, publicado_em, legenda) values ($1,$2,$3,$4,$5)
           on conflict (rede, post_id) do update set ativo=true, motivo=case when conteudo.vigia_posts.motivo='quente' and $3='recente' then 'recente' else conteudo.vigia_posts.motivo end, atualizado_em=now()`,
    [rede, id, motivo, publicado || null, (legenda || '').slice(0, 80)]);
}

// ---- comentários novos de um post ----
async function processarPost(rede, postId, primeiraVez) {
  const lista = rede === 'instagram'
    ? (await get(`${G}${postId}/comments?fields=id,text,timestamp,from,username&limit=50&access_token=${tk.leitorIG}`)).data || []
    : (await get(`${G}${postId}/comments?order=reverse_chronological&filter=stream&fields=id,message,created_time,from&limit=50&access_token=${tk.pagina}`)).data || [];
  if (!lista.length) return;
  const ids = lista.map(c => c.id);
  const vistos = new Set((await q("select comment_id from conteudo.vigia_comentarios where rede=$1 and comment_id = any($2)", [rede, ids])).map(r => r.comment_id));
  const limite = Date.now() - JANELA_H * 3600e3;
  const lote = []; // gravação em lote (1 pedido ao banco por post)
  for (const c of lista) {
    if (vistos.has(c.id)) continue;
    const texto = rede === 'instagram' ? c.text : c.message;
    const quando = c.timestamp || c.created_time;
    const deFora = rede === 'instagram' ? !(c.from && String(c.from.id) === IG_ID) : !(c.from && String(c.from.id) === FB_PAGE);
    let acao = 'ignorado', resultado = null;
    if (primeiraVez) acao = 'semeado';                                   // já existia quando o post entrou na vigia
    else if (deFora && new Date(quando).getTime() >= limite && temGatilho(texto)) {
      const payload = rede === 'instagram'
        ? { object: 'instagram', entry: [{ id: IG_ID, time: Date.now(), changes: [{ field: 'comments', value: { id: c.id, text: texto, from: c.from || { username: c.username }, media: { id: postId } } }] }] }
        : { object: 'page', entry: [{ id: FB_PAGE, time: Date.now(), changes: [{ field: 'feed', value: { item: 'comment', verb: 'add', comment_id: c.id, message: texto, from: c.from, post_id: postId } }] }] };
      const r = await fetch(DISPARADOR, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(ENV.DISPARADOR_SECRET ? { 'x-vigia-secret': ENV.DISPARADOR_SECRET } : {}) }, body: JSON.stringify(payload) });
      resultado = await r.json().catch(async () => ({ status: r.status }));
      acao = 'repassado'; estado.repassados++;
      log('repassado', rede, postId, JSON.stringify(texto).slice(0, 40), JSON.stringify(resultado).slice(0, 200));
    }
    lote.push([c.id, (texto || '').slice(0, 500), quando, acao, resultado ? JSON.stringify(resultado) : null]);
  }
  if (lote.length) await q(`insert into conteudo.vigia_comentarios (rede, comment_id, post_id, texto, comentado_em, acao, resultado)
      select $1, x.id, $2, x.texto, x.quando::timestamptz, x.acao, x.res::jsonb
      from unnest($3::text[], $4::text[], $5::text[], $6::text[], $7::text[]) as x(id, texto, quando, acao, res)
      on conflict do nothing`, [rede, postId, lote.map(l => l[0]), lote.map(l => l[1]), lote.map(l => l[2]), lote.map(l => l[3]), lote.map(l => l[4])]);
}

// ---- um ciclo ----
async function ciclo() {
  await carregar();
  // Instagram: recentes (1 pedido) + quentes (1 pedido a cada 50)
  if (VIGIAR_IG) {
  const rec = (await get(`${G}${IG_ID}/media?fields=id,comments_count,timestamp,caption&limit=30&access_token=${tk.leitorIG}`)).data || [];
  for (const m of rec) await upsertPost('instagram', m.id, 'recente', m.timestamp, m.caption);
  await checarContagens('instagram', Object.fromEntries(rec.map(m => [m.id, m.comments_count])), id => `${G}?ids=${id}&fields=comments_count&access_token=${tk.leitorIG}`, x => x.comments_count);
  }
  // Facebook: recentes + quentes
  if (!VIGIAR_FB) return;
  // feed em 2 páginas de 15 (lote maior a API recusa); FB só vigia os 30 recentes (consulta por vários ids foi descontinuada)
  const f1 = await get(`${G}${FB_PAGE}/feed?fields=id,created_time,message,comments.summary(true).limit(0)&limit=15&access_token=${tk.pagina}`);
  const f2 = f1.paging && f1.paging.next ? await get(f1.paging.next) : { data: [] };
  const fb = [...(f1.data || []), ...(f2.data || [])];
  for (const p of fb) await upsertPost('facebook', p.id, 'recente', p.created_time, p.message);
  await checarContagens('facebook', Object.fromEntries(fb.map(p => [p.id, p.comments && p.comments.summary ? p.comments.summary.total_count : 0])), id => `${G}?ids=${id}&fields=comments.summary(true).limit(0)&access_token=${tk.pagina}`, x => x.comments && x.comments.summary ? x.comments.summary.total_count : 0);
}

async function checarContagens(rede, jaSei, urlLote, ler) {
  const LOTE = rede === 'facebook' ? 10 : 50; // a API de Página recusa lote grande ("reduce the amount of data")
  const lista = await vigiados(rede);
  estado.vigiados = lista.length;
  const atual = { ...jaSei };
  const falta = rede === 'facebook' ? [] : lista.map(p => p.post_id).filter(id => !(id in atual));
  for (let i = 0; i < falta.length; i += LOTE) {
    const r = await get(urlLote(falta.slice(i, i + LOTE).join(',')));
    for (const [id, x] of Object.entries(r)) atual[id] = ler(x);
  }
  for (const p of lista) {
    const n = atual[p.post_id];
    if (n === undefined || n === p.ultimo_count) continue;
    const primeira = p.ultimo_count === null;
    if (n > (p.ultimo_count || 0) || primeira) await processarPost(rede, p.post_id, primeira);
    await q("update conteudo.vigia_posts set ultimo_count=$3, ultima_mudanca=now(), atualizado_em=now() where rede=$1 and post_id=$2", [rede, p.post_id, n]);
  }
}

// ---- revisão diária: post antigo que voltou a receber comentário entra; parado há N dias sai ----
async function revisaoDiaria() {
  await carregar();
  let url = `${G}${IG_ID}/media?fields=id,comments_count,timestamp,caption&limit=100&access_token=${tk.leitorIG}`, n = 0;
  while (url && n < 5000) {
    const r = await get(url); url = r.paging && r.paging.next;
    const d = r.data || []; n += d.length;
    // posts fora da vigia ficam guardados inativos só com a contagem; se subir, viram "quente" (1 pedido por página de 100)
    if (d.length) await q(`insert into conteudo.vigia_posts (rede, post_id, motivo, ultimo_count, publicado_em, legenda, ativo)
        select 'instagram', x.id, 'arquivo', x.n, x.pub::timestamptz, x.leg, false from unnest($1::text[], $2::int[], $3::text[], $4::text[]) as x(id, n, pub, leg)
        on conflict (rede, post_id) do update set
          ativo = conteudo.vigia_posts.ativo or excluded.ultimo_count > coalesce(conteudo.vigia_posts.ultimo_count, excluded.ultimo_count),
          motivo = case when not conteudo.vigia_posts.ativo and excluded.ultimo_count > coalesce(conteudo.vigia_posts.ultimo_count, excluded.ultimo_count) then 'quente' else conteudo.vigia_posts.motivo end,
          ultima_mudanca = case when not conteudo.vigia_posts.ativo and excluded.ultimo_count > coalesce(conteudo.vigia_posts.ultimo_count, excluded.ultimo_count) then now() else conteudo.vigia_posts.ultima_mudanca end`,
      [d.map(m => m.id), d.map(m => m.comments_count || 0), d.map(m => m.timestamp), d.map(m => (m.caption || '').slice(0, 80))]);
    await espera(500);
  }
  // sai da vigia: quente/recente sem comentário novo há QUENTE_DIAS (os 30 mais recentes voltam sozinhos no ciclo)
  await q(`update conteudo.vigia_posts set ativo=false, motivo='arquivo' where ativo and coalesce(ultima_mudanca, publicado_em, atualizado_em) < now() - ($1 || ' days')::interval`, [String(QUENTE_DIAS)]);
  await q("delete from conteudo.vigia_comentarios where visto_em < now() - interval '120 days'");
  log('revisão diária: IG posts lidos', n);
}

// ---- caixa de entrada: busca @/nome/foto dos contatos novos (até 20 por minuto) ----
async function enriquecer() {
  await carregar();
  const lista = await q("select id, rede, usuario_id from conteudo.social_contatos where enriquecido_em is null order by ultimo_em desc limit 20");
  for (const c of lista) {
    let d = {};
    try {
      d = c.rede === 'instagram'
        ? await get(`https://graph.instagram.com/v21.0/${c.usuario_id}?fields=username,name,profile_pic&access_token=${tk.ig}`)
        : await get(`${G}${c.usuario_id}?fields=first_name,last_name,profile_pic&access_token=${tk.pagina}`);
    } catch (e) { d = { erro: e.message }; }
    const nome = d.name || [d.first_name, d.last_name].filter(Boolean).join(' ') || null;
    await q("update conteudo.social_contatos set username=coalesce($2,username), nome=coalesce($3,nome), foto_url=coalesce($4,foto_url), enriquecido_em=now() where id=$1",
      [c.id, d.username || null, nome, d.profile_pic || null]);
  }
  if (lista.length) log('enriquecidos', lista.length);
}

// ---- laço ----
async function laco() {
  let proximaRevisao = 0;
  for (;;) {
    const t0 = Date.now();
    try {
      estado.pausado = (ENV.PAUSADO || '').toLowerCase() === 'true';
      if (!estado.pausado) {
        if (VIGIAR_IG && Date.now() > proximaRevisao) { proximaRevisao = Date.now() + 24 * 3600e3; revisaoDiaria().catch(e => { estado.erros++; estado.ultimoErro = 'revisao: ' + e.message; log('erro revisão', e.message); }); }
        await ciclo();
        if (estado.ciclos % 6 === 0) await enriquecer().catch(e => log('erro enriquecer', e.message));
        estado.ciclos++; estado.ultimoCiclo = new Date().toISOString();
        if (estado.ciclos % 60 === 1) log('vivo — ciclos', estado.ciclos, 'vigiados', estado.vigiados, 'repassados', estado.repassados, 'erros', estado.erros, 'ciclo levou', Date.now() - t0, 'ms');
      }
    } catch (e) {
      estado.erros++; estado.ultimoErro = e.message; log('erro ciclo', e.message);
      if (/^Graph (4|17|32|613):/.test(e.message)) await espera(5 * 60e3); // limite de chamadas da Meta: respira 5 min
    }
    await espera(Math.max(1000, INTERVALO - (Date.now() - t0)));
  }
}

http.createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(estado)); }).listen(Number(ENV.PORT || 8000));
log('vigia-comentarios no ar — intervalo', INTERVALO / 1000, 's | IG', VIGIAR_IG, '| FB', VIGIAR_FB);
laco();
