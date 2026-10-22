# RocketLab-case-V-Credit

Pipeline de dados em notebooks Databricks, organizado na arquitetura medalhão (Bronze, Silver e Gold), que transforma as bases dispersas do atendimento da V-Credit em um modelo consistente e pronto para análise. Case final do programa Rocket Lab, da Visagio.

## Contexto

A V-Credit enfrenta aumento do volume de atendimentos, custos operacionais crescentes e queda na satisfação dos clientes. As informações da operação estão espalhadas em bases de sistemas diferentes, com inconsistências de formato, codificação e nomenclatura, o que dificulta acompanhar indicadores como volume de chamados, custo médio por atendimento e satisfação.

O projeto estrutura essas bases em camadas, documenta as inconsistências encontradas e os tratamentos aplicados, e entrega um modelo dimensional sobre o qual são feitas as análises de negócio. Os dados cobrem atendimentos de janeiro de 2025 a julho de 2026.

## Estrutura do repositório

```
00_setup/
    Organization.ipynb               cria catálogo, schemas e volume
01_bronze/
    Bronze_Layer.ipynb               ingestão dos CSVs
    analysis/
        Bronze_Analysis_*.ipynb      análise exploratória de cada tabela da Bronze
02_silver/
    Silver_Notebook_*.ipynb          tratamento de cada tabela (um notebook por tabela)
03_gold/
    Gold_Layer.ipynb                 modelo dimensional e validações
04_business_analysis/
    Analises_de_Negocio.ipynb        análises sobre a Gold, por parte interessada
README.md
```

Os notebooks de `analysis` e de `04_business_analysis` não fazem parte do pipeline. Eles documentam as investigações que embasam os tratamentos e as conclusões de negócio.

## Arquitetura

Todo o processamento é feito no Databricks Free Edition, no catálogo `vcredit_medallion`.

| Camada | Schema | Conteúdo |
|---|---|---|
| Bronze | `bronze` | Os 8 CSVs carregados sem tratamento, com a coluna `ingestion_datetime` |
| Silver | `silver` | Uma tabela tratada por base de origem, com prefixo `tb_` |
| Gold | `gold` | Modelo dimensional em esquema estrela: `fato_chamado` e 7 dimensões |

### Bronze

`Bronze_Layer` lê os CSVs do volume `bronze.landing` e grava uma tabela Delta para cada um: `chamados`, `chamados_hora`, `clientes`, `custos`, `pesquisa_satisfacao`, `canais`, `base_motivos` e `base_atendentes`.

A gravação é feita em modo append, preservando o histórico de cargas. Cada tabela é lida com o encoding do seu arquivo: `chamados_hora` e `clientes` em ISO-8859-1, as demais em UTF-8. O padrão de leitura `*{tabela}.csv` exige que o nome do arquivo termine com o nome da tabela, o que evita que a leitura de `chamados` inclua também o arquivo `chamados_hora.csv`.

### Silver

Cada tabela da Bronze tem um notebook próprio na Silver, que trata os problemas identificados na análise exploratória e grava a tabela `tb_<nome>`. Como a Bronze é append, cada notebook trabalha apenas com a versão mais recente de cada registro. Os notebooks que usam validações interrompem a execução quando alguma regra é violada, antes da gravação.

### Gold

`Gold_Layer` monta o esquema estrela:

| Tabela | Descrição |
|---|---|
| `fato_chamado` | Uma linha por chamado, com as chaves das dimensões, tempos de espera e de atendimento, custo e nota |
| `dim_motivo` | Motivo, categoria e criticidade |
| `dim_canal` | Canal, tipo e etapa do funil de atendimento |
| `dim_atendente` | Atendente, nível, admissão, desligamento e status |
| `dim_cliente` | Cliente, região, faixa etária, cadastro e vencimento da fatura |
| `dim_data` | Calendário que cobre todo o período dos chamados |
| `dim_hora` | Todos os minutos do dia |
| `dim_status` | Resolvido, não resolvido com atendimento e abandonado na espera |

Toda dimensão tem um membro "Não Informado", com chave -1, para onde apontam os chamados sem correspondência. O notebook termina com consultas de validação que conferem a integridade das chaves, a unicidade dos registros e a coerência dos dados da fato.

## Execução

Os notebooks devem ser executados nesta ordem:

1. `Organization`
2. `Bronze_Layer`
3. Silver de `canais`, `base_motivos`, `base_atendentes`, `clientes`, `chamados`, em qualquer ordem
4. Silver de `custos`, `pesquisa_satisfacao`, `chamados_hora`
5. `Gold_Layer`

A mesma ordem é usada nas tarefas do job de atualização. Os CSVs de origem devem estar no volume `vcredit_medallion.bronze.landing` antes da primeira execução.

O pipeline é idempotente: executá-lo de novo sobre os mesmos arquivos produz as mesmas tabelas na Silver e na Gold, porque a Silver considera apenas a carga mais recente de cada registro e as duas camadas são regravadas a cada execução.


## Análises de negócio

`Analises_de_Negocio` reúne as consultas sobre a Gold, organizadas pelas dores relatadas por cada parte interessada da V-Credit: Gerente Financeiro, Coordenadora de Customer Success, Diretor de Tecnologia, Diretora de Marketing e atendentes.

## Visualização

O Databricks Free Edition não tem integração nativa com o Power BI. Para os painéis, as tabelas da Gold são exportadas em CSV e importadas no Power BI, onde as relações entre a fato e as dimensões são recriadas.
