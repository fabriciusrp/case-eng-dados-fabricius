# Achados de Qualidade de Dados — Fontes Brutas

Este documento registra os problemas identificados na exploração de cada fonte em `sources/`,
antes de qualquer transformação. Serve de base para as decisões de tratamento aplicadas nas
camadas Bronze/Silver (ver `02_arquitetura_e_modelagem.md`).

Convenção usada abaixo: **Achado** (o que foi observado) → **Impacto** (por que importa) →
**Tratamento proposto** (o que será feito e em qual camada).

---

## 1. `cadastro_produtos_api_dump.json` (72 registros)

| Achado | Evidência | Impacto | Tratamento proposto |
|---|---|---|---|
| `product_id` com casing inconsistente | `p0013`, `p0026`, `p0039`, `p0052`, `p0065` em minúsculo | Quebra joins com `erp_pedidos_itens` se comparado case-sensitive | Normalizar para upper-case como chave de negócio na Silver |
| Registro duplicado por chave de negócio | `P0006` aparece 2x: "Produto 6" (`updated_at` 2025-11-02) e "Produto 6 revisado" (`updated_at` 2026-02-12) | Duplicidade de dimensão quebra granularidade 1 produto = 1 linha | Deduplicar mantendo o registro com `updated_at` mais recente (SCD1) |
| `status` inconsistente | Valores: `Ativo`, `ativo`, `inativo`, `descontinuado`, `null` (21 nulos) | Dificulta segmentação por status na Gold | Padronizar para minúsculo; nulo vira `"nao_informado"` |
| `list_price` com tipo misto | Maioria numérico, mas `P9999` tem `"N/A"` (string) | Coluna não pode ser tratada como DecimalType sem cast | Cast com tratamento de erro → `null` + flag de qualidade |
| `category` com encoding correto mas requer padronização | `Serviços` (contém acento) coexiste com `Software`, `Hardware`, `Assinatura` | Nenhum — apenas confirmar leitura UTF-8 correta na ingestão | Garantir `encoding="utf-8"` no reader; sem tratamento adicional |
| `name` nulo | `P9999` tem `name = null` | Registro de produto "genérico"/placeholder, sem nome | Manter, sinalizar como produto placeholder (mesmo ID usado como pedido "coringa" nos itens) |
| `subcategory` nulo | 6 registros | Análises por subcategoria perdem esses produtos | Preencher com `"nao_informado"` |
| `family` nulo | 13 registros | Idem | Preencher com `"nao_informado"` |

**Observação-chave:** `P9999` é claramente um produto "coringa"/placeholder (nome nulo, preço "N/A") usado propositalmente em vários pedidos — decisão: manter na dimensão de produtos com atributos sinalizados, não descartar (pedidos que o referenciam existem de verdade).

---

## 2. `erp_pedidos_cabecalho_2025.csv` (403 linhas de dados, `;`-delimited)

| Achado | Evidência | Impacto | Tratamento proposto |
|---|---|---|---|
| `order_id` com casing inconsistente | `o00021`, `o00042`, `o00084`, etc. (17 ocorrências em minúsculo) | Quebra granularidade se tratado como chave distinta de `O00021` | Normalizar para upper-case |
| Pedidos duplicados por chave de negócio | `O00011` aparece 2x com `status_order` diferente (`Faturado` → `cancelado`) e `last_update` diferente; `O00081` 2x (uma com `gross_amount` ausente); `O00121` 2x idêntico | Duplicidade real de linha de pedido | Deduplicar por `order_id` mantendo o registro com `last_update` mais recente |
| `status_order` com alta cardinalidade de formatação | `Faturado`/`faturado`, `EM_SEPARACAO`/`em separacao`, `entregue`, `cancelado`, e **64 nulos** | Sem padronização, cada variação vira uma categoria distinta em contagens de status | Normalizar para snake_case minúsculo (`faturado`, `em_separacao`, `entregue`, `cancelado`); nulo → `"nao_informado"` |
| `order_date` / `promised_date` com formatos mistos | `2025-02-24` (ISO), `17/10/2025` (BR dd/mm/yyyy), `2025/09/02` (yyyy/mm/dd) | Parse ingênuo gera datas erradas (ex.: confundir dia/mês) | Parser multi-formato explícito na Bronze→Silver, com fallback a `null` + flag se ambíguo |
| Data logicamente inválida | `order_id = O00121` (a segunda ocorrência) tem `order_date = "2025-13-40"` (mês 13, dia 40) | Quebra qualquer parse de data | Não é parseável em nenhum formato → vira `null` com flag `dq_invalid_date = true` |
| `gross_amount` com separador decimal BR (vírgula) | 14 linhas, ex.: `781,16`, `10189,38`, `7407,5` | Cast direto para double falha ou trunca | Normalizar vírgula→ponto antes do cast |
| `customer_code` com casing inconsistente | `c0136`, `c0102`, `c0085`, etc. | Quebra join com `crm_clientes_export` | Normalizar para upper-case |
| Cliente placeholder `C9999` | 8 pedidos referenciam `C9999`, que não existe no CRM | Órfão de FK — pedido sem cliente cadastrado | Manter pedido, cliente resolve para `"cliente_desconhecido"` na Gold via left join |
| `payment_details` é JSON aninhado em string | Coluna contém `"{""priority"": ..., ""source"": ...}"` | Dado semi-estruturado dentro de CSV | Parse do JSON string na Silver, extraindo `priority` e `source` como colunas |
| `net_amount` inconsistente com `gross - discount` | Ex.: `O00033`: gross=3465.27, discount=0.0, net=3511.39 (não bate); `O00396`: gross=4418.43, net=4508.3 | Sinal de erro de digitação/cálculo na origem | Não recalcular sobre a fonte — manter valor original e adicionar coluna calculada `net_amount_calculado` para conciliação/flag de divergência |

---

## 3. `erp_pedidos_itens_2025.csv` (995 linhas de dados)

| Achado | Evidência | Impacto | Tratamento proposto |
|---|---|---|---|
| `order_id`/`product_code` com casing inconsistente | Mesmo padrão do cabeçalho e do catálogo | Quebra joins | Normalizar para upper-case |
| Linhas duplicadas exatas | `O00002`/item 2/`P0056` aparece 2x idêntico; `o00062`/item 2/`P0006` aparece 2x (uma com `quantity` nula) | Duplicaria receita/quantidade se somado direto | Deduplicar por `(order_id, item_seq)` — ver próximo achado sobre por que `product_code` não entra na chave |
| `quantity` nula/em branco | 1 linha (`o00062`, item 2) | Quebra agregações de volume | `null` tratado explicitamente; excluído de somas de quantidade, mantido com flag |
| `quantity` negativa | 12 linhas (ex.: `P0030 qty=-1`, `P0009 qty=-1`) | Provável estorno/devolução ou erro de lançamento — não fica claro qual | Manter e sinalizar (`dq_negative_quantity`); não inverter automaticamente sem regra de negócio confirmada |
| `quantity` zero | 8 linhas | Item sem efeito em receita, mas pode representar item cancelado antes da entrega | Manter, não excluir (`total_item` também é 0, consistente) |
| `unit_price` com vírgula decimal | 31 linhas, ex.: `"1274,78"` | Mesma classe de problema do cabeçalho | Normalizar vírgula→ponto |
| `item_status` inconsistente + muitos nulos | `ativo`/`Ativo` (494 juntos) vs `cancelado` (243) vs **258 nulos** (26%) | Quase 1/4 dos itens sem status | Padronizar casing; nulo → `"nao_informado"` (não inferir a partir do pedido pai) |
| Produto órfão **e** colisão de grão | `O00044`/item_seq 3 aparece 2x: uma linha com `P0047` (existe no catálogo), outra com `P8888` (não existe) — **mesma** quantidade, preço e total nas duas | Se `product_code` fizesse parte da chave de dedup, as duas sobreviveriam como itens distintos, inflando quantidade/receita do pedido em dobro | Grão de deduplicação é `(order_id, item_seq)`, não `(order_id, item_seq, product_code)`; desempate prefere a linha cujo `product_code` existe na dimensão de produtos → mantém `P0047`, descarta `P8888` |
| `total_item` inconsistente com `quantity × unit_price` | Ex.: `O00127` item 4: qty=10, price=2484.15 → esperado 24841.50, mas total_item=24840.02 | Pequenas divergências de arredondamento/digitação na origem | Manter valor original da fonte; não recalcular por padrão (decisão documentada), mas expor `total_item_calculado` para auditoria |

---

## 4. `legado_regioes_pipe.txt` (8 linhas, pipe-delimited, arquivo "legado")

| Achado | Evidência | Impacto | Tratamento proposto |
|---|---|---|---|
| Região duplicada com chave alternativa | `SE` aparece 2x: uma com `state=SP`, outra com `state=sao paulo` (mesmo estado, grafia diferente) | Duplicata funcional da região Sudeste — após `normalize_state_to_uf` as duas linhas ficam idênticas em todas as colunas relevantes | Deduplicar por `regional_code` normalizado; como as linhas são equivalentes após normalização de estado, qualquer uma pode sobreviver sem perda de informação |
| Região "Sul" com código alternativo | `sul` (minúsculo) com `regional_name="Região Sul"`, `state="Sta Catarina"` coexiste com `S`/`Sul`/`SC` — mesmo gestor (Rafael Souza) nas duas | Tanto a própria tabela de regiões quanto `vendedores.regional_code` usam os dois códigos para a mesma região | Mapear `sul` → `S` (`REGIONAL_CODE_ALIAS` em `utils.py`), aplicado tanto em `silver.regioes` (onde as duas linhas colapsam em 1 após a normalização) quanto em `silver.vendedores.regional_code`. Desempate em `silver.regioes` prefere o nome curto ("Sul", consistente com Norte/Nordeste/Sudeste/Centro-Oeste) sobre "Região Sul" |
| Região "lixo" | `XX` com `regional_name`, `state` vazios, `manager_name="Sem gestor"`, `active_flag=0` | Registro inativo, provavelmente placeholder de erro/teste | Manter na dimensão mas marcado `active_flag=0`; não usar em análises de região ativa |
| `state` com grafias variadas | `Sta Catarina` (no cabeçalho de pedidos/entregas, não neste arquivo) | Ver problema semelhante em `crm_clientes` e `logistica_entregas` | Tabela de normalização de estado compartilhada entre fontes (ver item 6 e 9) |

---

## 5. `vendedores.csv` (42 linhas de dados)

| Achado | Evidência | Impacto | Tratamento proposto |
|---|---|---|---|
| Vendedor duplicado por ID com atributo divergente | `V004` aparece 2x: `canal_id=CH02` e `canal_id=CH99` (canal que não existe em `comercial_canais`) | Ambíguo qual é o canal correto do vendedor | Deduplicar mantendo a primeira ocorrência válida (`CH02`, que existe na dimensão canal); descartar a linha com `CH99` órfão, documentando a decisão |
| Vendedor duplicado por ID com nome divergente | `V008` aparece 2x: "Vendedor 8" e "Vendedor 8 duplicado" (mesmo canal/regional/hire_date) | Claramente um erro de exportação duplicada | Deduplicar mantendo o registro com nome "canônico" (sem sufixo "duplicado") |
| `canal_id` nulo | 5 vendedores | Vendedor sem canal atribuído | `"nao_informado"` |
| `canal_id` com casing inconsistente | `ch07` (minúsculo) | Quebra join com `comercial_canais` (`CH01`...`CH06`, mas nota: `ch07` também aparece lá) | Normalizar para upper-case em ambas as fontes |
| `regional_code` nulo | 3 vendedores | Vendedor sem região | `"nao_informado"` |
| `regional_code = "sul"` (minúsculo) | Ver achado #4 acima | Precisa mapear para `S` | Aplicar de-para de região |
| `status` inconsistente + nulos | `Ativo`/`ativo` (22 juntos) vs `inativo` (9) vs **11 nulos** | ~26% sem status | Padronizar casing; nulo → `"nao_informado"` |
| `hire_date` com formatos mistos | ISO e `dd/mm/yyyy` misturados | Mesmo problema de parsing de data do pedido | Parser multi-formato |

---

## 6. `logistica_entregas.json` (322 registros)

| Achado | Evidência | Impacto | Tratamento proposto |
|---|---|---|---|
| `delivery_id` duplicado | `D00004` 2x (uma aponta para `O00250`, outra para `O99999` — pedido inexistente); `D00021` 2x (uma com data válida, outra com `shipped_at="31/02/2025 10:00"`, data que não existe) | Duplicidade + uma das duplicatas é claramente corrompida | Deduplicar por `delivery_id`, descartando a versão com dados inválidos (data inexistente / pedido órfão) quando houver uma alternativa válida |
| `order_ref` órfão | `O99999` não existe em `erp_pedidos_cabecalho` | Entrega sem pedido correspondente | Manter registro de entrega isolado (não teria como aparecer em fatos de pedido-entrega); documentar como órfão conhecido |
| `order_ref` com casing inconsistente | `o00020`, `o00023`, `o00025`, etc. | Quebra join com pedidos | Normalizar para upper-case |
| `delivery_status` inconsistente + muitos nulos | `in_transit`, `delivered`/`Delivered`, `cancelled`, `atrasado` (este em português, os outros em inglês!) — **60 nulos** | Mistura de idiomas na mesma coluna + quase 20% sem status | Padronizar para um único vocabulário (ex.: `in_transit`, `delivered`, `cancelled`, `delayed`); nulo → `"nao_informado"` |
| `carrier.mode` inconsistente | `RodoviÃ¡rio`→`Rodoviário`/`rodoviario`, `AÃ©reo`→`Aéreo`; **65 nulos** | Mesma transportadora, grafias diferentes do modal | Padronizar casing/acentuação; nulo → `"nao_informado"` |
| `carrier.name` nulo | 72 registros (~22%) | Entrega sem transportadora identificada | `"nao_informado"` |
| `cost` com valor sentinela textual | 10 registros com `cost = "unknown"` (string) em vez de número | Cast para double quebra ou vira null silencioso | Cast com tratamento explícito; `"unknown"` → `null` + flag `dq_unknown_cost` |
| `destination.state` com altíssima cardinalidade para a mesma UF | Ex. para Santa Catarina: `SC`, `Santa Catarina`, `santa catarina`, `Sta Catarina`, `S. Catarina`; para São Paulo: `SP`, `Sao Paulo`, `São Paulo`, `sao paulo` | Análise "por região/estado" fica pulverizada em dezenas de categorias falsas | Tabela de normalização estado→UF (case-insensitive, com todas as variações de abreviação mapeadas) |
| Data logicamente inválida | `shipped_at = "31/02/2025 10:00"` (fevereiro não tem dia 31) | Quebra parsing de data | `null` + flag `dq_invalid_date` |

---

## 7. `atendimento_ocorrencias.ndjson` (270 registros)

| Achado | Evidência | Impacto | Tratamento proposto |
|---|---|---|---|
| `event_type` inconsistente + nulos | `delay`/`Delay` (mistura casing), `refund`, `troca` (português!), `cancel_request`, `complaint`; **37 nulos** | Mistura de idioma (troca vs refund/cancel_request) e casing | Padronizar vocabulário único; nulo → `"nao_informado"` |
| `severity` inconsistente + nulos | `high`/`High` (mistura), `medium`, `low`; **59 nulos** (~22%) | Prioriza atendimento de forma inconsistente se usado sem tratar | Padronizar minúsculo; nulo → `"nao_informado"` |
| `status` inconsistente + nulos | `open`/`Open`, `closed`; **75 nulos** (~28%) | Maior taxa de nulo entre as fontes | Padronizar minúsculo; nulo → `"nao_informado"` |
| `created_at` com formatos mistos | `2025-04-01 17:00:00`, `01/11/2025 16:00`, `2025/02/23` (só data, sem hora) | Parsing multi-formato necessário, com hora ausente em parte dos registros | Parser multi-formato; quando hora ausente, assumir `00:00:00` e sinalizar `dq_time_missing` |
| Coluna extra não documentada no profiling inicial | `metadata`, `customer_code` aparecem no schema além dos 6 campos originalmente visíveis na amostra | Confirma que o ndjson tem mais atributos que o esperado — necessário `inferSchema`/schema explícito cobrindo todos os campos | Definir schema explícito incluindo `metadata` (provavelmente struct/JSON aninhado) e `customer_code` |
| Nenhum órfão de `order_id` | Todos os `order_id` existem em `erp_pedidos_cabecalho` | — | Nenhum tratamento necessário |

---

## 8. `comercial_canais.xlsx` (8 linhas, aba "canais")

| Achado | Evidência | Impacto | Tratamento proposto |
|---|---|---|---|
| Canal duplicado por ID com atributos conflitantes | `CH05` aparece 2x: "E-commerce"/"digital"/"sim" e "ecommerce"/"Digital"/"sim" com observação explícita **"duplicado conflitante"** | Fonte já sinaliza intencionalmente o conflito | Deduplicar mantendo a primeira ocorrência ("E-commerce"), descartando a marcada como duplicata |
| Canal sem nome | `CH06` com `nome_canal = null`, observação **"nome ausente"** | Dimensão incompleta | Manter registro, `nome_canal = "nao_informado"`, sinalizado |
| `id_canal` com casing inconsistente | `ch07` (minúsculo), observação **"id em lowercase"** | Quebra join com `vendedores.canal_id` que também usa `ch07`/`CH0x` misturado | Normalizar para upper-case (mesma regra aplicada em `vendedores`) |
| `ativo` como texto livre inconsistente | `sim`/`Sim`/`SIM`, `nao`, `null` (para `ch07`) | Não é booleano usável diretamente | Normalizar para boolean: `{sim→true, nao→false, null→"nao_informado"/false com flag}` |
| `tipo_canal` com casing inconsistente | `Direto`, `Indireto`, `Digital`/`digital` | Pulveriza contagens por tipo de canal | Padronizar capitalização |

**Observação:** este arquivo já vem com uma coluna `observacao` que **documenta as próprias inconsistências propositalmente inseridas** (nome ausente, id lowercase, duplicado conflitante) — forte sinal de que é um dataset de teste construído para avaliar tratamento de qualidade.

---

## 9. `crm_clientes_export.xlsx` (183 linhas, aba única)

| Achado | Evidência | Impacto | Tratamento proposto |
|---|---|---|---|
| ~~Encoding corrompido~~ — **falso positivo, verificado e descartado** | Na inspeção inicial via `print()`/pandas no console do Windows, valores como `Florianópolis`, `Educação`, `Saúde`, `Uberlândia` apareciam como `Florian�polis`, `Educa��o` etc. Verificação direta dos **codepoints Unicode** (`ord(ch)`) confirmou que a string em memória está correta (`ó`=U+00F3, `ã`=U+00E3, `ç`=U+00E7, `ú`=U+00FA, ...) — o problema era só a *code page* do console do Windows na hora de exibir, não os dados | Nenhum — decisão registrada para não aplicar nenhum "conserto" de encoding desnecessário sobre um dado que já está correto | Nenhum tratamento; apenas garantir que a leitura use `openpyxl`/UTF-8 e que qualquer log/print de diagnóstico não seja confundido com o dado real |
| `customer_id` duplicado com atributos divergentes | `C0010` 2x (`segmento` nulo vs `"Financeiro"`, `updated_at` diferente); `C0025` 2x (`estado` diferente: `RJ` vs `Sta Catarina` — cidade Niterói é RJ, então a segunda está errada); `c0051`/`C0051` 2x (email difere: um válido, outro `"duplicado_sem_arroba.com"`) | Múltiplos padrões de duplicidade: SCD real, erro de estado, erro de email | Deduplicar por `customer_id` (normalizado upper) mantendo o registro com `updated_at` mais recente; nos casos de campo mais completo/correto na versão antiga (ex. estado correto), preferir o valor não-nulo/mais plausível quando `updated_at` empatar — decisão registrada caso a caso não é escalável, então a regra geral aplicada é **"mais recente vence"**, com a divergência do estado do `C0025` documentada como limitação conhecida |
| `customer_id` com casing inconsistente | `c0051` minúsculo | Quebra join com pedidos | Normalizar upper-case |
| `status_cliente` inconsistente + nulos | `Ativo`/`ativo`/`ATIVO` (3 variações!), `Inativo`/`inativo`; **35 nulos** (~19%) | Pior caso de fragmentação de casing do dataset inteiro | Padronizar minúsculo; nulo → `"nao_informado"` |
| `segmento` nulo | 34 registros (~19%) | — | `"nao_informado"` |
| `porte` inconsistente + nulos | `Grande`/`grande`, `Média`, `Pequena`; **39 nulos** (~21%) | — | Padronizar capitalização; nulo → `"nao_informado"` |
| `estado` com altíssima cardinalidade | Mesmo padrão do achado #6 (SC/SP/PR/RJ/MG todos com múltiplas grafias) | Análise geográfica pulverizada | Reaproveitar a mesma tabela de-para estado↔UF usada em `logistica_entregas` |
| `data_cadastro` com formatos mistos | ISO e `dd/mm/yyyy` e `yyyy/mm/dd` | Mesmo padrão de todas as datas do case | Parser multi-formato |
| `email` nulo | 4 registros | Contato ausente | Manter nulo, sem preenchimento sintético |
| `email` com formato inválido | ~10 registros sem `@` (ex.: `cliente29empresa.com`) ou o caso do `C0051` duplicado (`duplicado_sem_arroba.com`) | Emails não utilizáveis para contato | Manter valor original + flag `dq_invalid_email`; não tentar "consertar" o endereço automaticamente |
| Cliente órfão referenciado em pedidos | `C9999` usado em 8 pedidos, não existe nesta base | Ver achado #2 | Resolvido via left join com fallback `"cliente_desconhecido"` na Gold |

---

## Padrões transversais (aparecem em quase todas as fontes)

1. **Casing inconsistente em IDs de negócio** (`order_id`, `product_code`, `customer_id`, `seller_id`, `canal_id`, `regional_code`) — tratamento único: normalizar todas as chaves para upper-case na entrada da Silver, antes de qualquer join.
2. **Status/enums com casing e idioma misturados e alta taxa de nulo** (10–28% dependendo da fonte) — tratamento único: função de padronização (lower + trim) + `"nao_informado"` para nulo, nunca inferência de valor.
3. **Datas em 3+ formatos diferentes** (`yyyy-MM-dd`, `dd/MM/yyyy`, `yyyy/MM/dd`, com/sem hora) e ocasionalmente datas logicamente inválidas (`31/02`, mês `13`) — tratamento único: função de parse multi-formato com fallback `null` + flag de qualidade, nunca descarte silencioso da linha inteira.
4. **Números com separador decimal BR (vírgula)** em `gross_amount`, `unit_price` — tratamento único: `regexp_replace(',', '.')` antes do cast.
5. **Nomes de estado/UF com dezenas de grafias** (sigla, nome completo, abreviação parcial, minúsculo) — tratamento único: dimensão de-para estado↔UF compartilhada entre `logistica_entregas` e `crm_clientes_export`.
6. **Duplicidade de registros por chave de negócio**, com padrões variados (mesmo conteúdo, conteúdo divergente por campo, ou sufixo "duplicado" explícito) — tratamento caso a caso mas com regra-padrão: manter o registro mais recente por `updated_at`/`last_update` quando existir, senão a primeira ocorrência.
7. **Chaves estrangeiras órfãs** (produto `P8888`, cliente `C9999`, pedido `O99999`, canal `CH99`) — tratamento único: nunca descartar o fato; resolver a dimensão para um valor `"desconhecido"` explícito via left join, preservando 100% dos fatos.
8. **Valores sentinela textuais em colunas numéricas** (`"N/A"`, `"unknown"`) — tratamento único: cast defensivo → `null` + flag de qualidade dedicada por coluna.

Essas decisões serão implementadas como funções utilitárias reutilizáveis na camada Silver
(ver `02_arquitetura_e_modelagem.md` para o desenho completo bronze → silver → gold).
