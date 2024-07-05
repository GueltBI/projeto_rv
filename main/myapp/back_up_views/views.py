from django.shortcuts import render, redirect, resolve_url
from django.contrib.auth import login, authenticate, logout
from .forms import RegisterForm, LoginForm
from django.views.decorators.cache import never_cache
from django.contrib.auth.decorators import login_required
import pandas as pd
import locale
from django.utils.safestring import mark_safe
import psycopg2
import psycopg2.pool
import numpy as np


# Configuração do pool de conexões
connection_pool = psycopg2.pool.SimpleConnectionPool(
    1, 20,
    user="gueltdbmaster",
    password="gueltdbpassword123",
    host="gueltdatabase-01.c3qxaey67bfc.us-east-1.rds.amazonaws.com",
    port="5432",
    database="postgres",
    options="-c search_path=guelt_db_schema"
)





def home_view(request):
    return render(request, 'myapp/home.html')

def register_view(request):
    if request.method == 'POST':
        form = RegisterForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect('home')
    else:
        form = RegisterForm()
    return render(request, 'myapp/register.html', {'form': form})

def login_view(request):
    next_url = request.GET.get('next', '/info/')  # Define um valor padrão para next
    if request.method == 'POST':
        form = LoginForm(request, data=request.POST)
        if form.is_valid():
            username = form.cleaned_data.get('username')
            password = form.cleaned_data.get('password')
            user = authenticate(request, username=username, password=password)
            if user is not None:
                login(request, user)
                next_url = request.POST.get('next', '/info/')  # Define um valor padrão para next
                return redirect(next_url)
    else:
        form = LoginForm()
    return render(request, 'myapp/login.html', {'form': form, 'next': next_url})

def logout_view(request):
    logout(request)
    return redirect('login')



########################## FUNÇÕES DE PRIMEIRA PÁGINA ######################################


def get_filtered_data(conta=None, produto=None, ativo=None):
    connection = None
    cursor = None

    try:
        connection = connection_pool.getconn()
        connection.autocommit = True
        cursor = connection.cursor()

        # Construir a consulta SQL dinamicamente com base nos filtros fornecidos
        sql = """
        SELECT 
            pa.conta, 
            pa.ativo, 
            pa.qtde, 
            pa.preco_medio, 
            pa.produto, 
            tp.preco AS preco_atual
        FROM 
            guelt_main.posicao_atual_rv pa
        LEFT JOIN 
            guelt_main.ticker_preco tp 
        ON 
            pa.ativo = tp.ticker
        WHERE 
            1=1
        """
        params = []
        if conta:
            sql += " AND pa.conta = %s"
            params.append(conta)

        if produto and produto != 'all':
            sql += " AND pa.produto = %s"
            params.append(produto)

        if ativo and ativo != 'all':
            sql += " AND pa.ativo = %s"
            params.append(ativo)

        cursor.execute(sql, params)
        plan = cursor.fetchall()

        # Criar DataFrame com os dados
        df = pd.DataFrame(plan, columns=[
            "conta", 
            "ativo", 
            "qtde", 
            "preco_medio", 
            "produto", 
            "preco_atual"
        ])

        # Ajustes nas colunas
        df['qtde'] = pd.to_numeric(df['qtde'], errors='coerce')
        df['preco_medio'] = pd.to_numeric(df['preco_medio'], errors='coerce')
        df['preco_atual'] = pd.to_numeric(df['preco_atual'], errors='coerce')

        # Calculando a rentabilidade e o ganho financeiro
        df['ganho_financeiro'] = (df['preco_atual'] * df['qtde']) - (df['preco_medio'] * df['qtde'])
        df['rentabilidade'] = (df['ganho_financeiro']) / (df['qtde'] * df['preco_medio']) * 100

        df = df[['conta', 'ativo', 'qtde', 'preco_medio', 'produto', 'preco_atual', 'ganho_financeiro', 'rentabilidade']]
        df = df.sort_values(by='ganho_financeiro', ascending=False)

        # Obter códigos de cliente distintos
        cursor.execute("SELECT DISTINCT conta FROM guelt_main.posicao_atual_rv")
        client_codes_all = [row[0] for row in cursor.fetchall()]

        return df, client_codes_all, df['produto'].unique(), df['ativo'].unique()

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection_pool.putconn(connection)


   
    ############### Segunda Consulta ###################################
def get_filtered_data2(id_cliente=None, produto=None, ativo=None):
    connection = None
    cursor = None

    try:
        connection = connection_pool.getconn()
        connection.autocommit = True
        cursor = connection.cursor()

        # Construir a consulta SQL dinamicamente com base nos filtros fornecidos
        sql = """
            SELECT id_cliente,
              ativo,
              qtd,
              corretagem, 
              volume, 
              produto, 
              lado, 
              "data", 
              preco, 
              volume_ajustado
            FROM guelt_main.rv_hubrv
            WHERE 
                1=1
            """

        params = []

        if id_cliente:
            sql += " AND id_cliente = %s"
            params.append(id_cliente)

        if produto and produto != 'all':
            sql += " AND produto = %s"
            params.append(produto)

        if ativo and ativo != 'all':
            sql += " AND ativo = %s"
            params.append(ativo)
                
        cursor.execute(sql, params)
        plan = cursor.fetchall()

        # Verificar se a consulta retornou resultados vazios
        if not plan:
            return pd.DataFrame(columns=[
                'id_cliente', 'Data de Entrada', 'ativo', 'produto',
                  'ganho_financeiro', 'divi_total', 'Data de Saída'
            ])

        # Criar DataFrame com os dados
        dff = pd.DataFrame(plan, columns=[ 
                                        "id_cliente", 
                                        "ativo", 
                                        "qtd", 
                                        "corretagem", 
                                        "volume", 
                                        "produto",
                                        "lado", 
                                        "data",
                                        "preco_atual",
                                        "volume_ajustado"])

        #FAZENDO AS MODIFICAÇÕES NECESSÁRIA PARA AS COLUNAS::
        dff['ativo'] = dff['ativo'].str.rstrip('F')
        dff['data'] = pd.to_datetime(dff['data'], format='%d-%m-%Y', errors='coerce')
        dff = dff.sort_values(by=['ativo', 'id_cliente', 'data'])
        dff['qtd'] = pd.to_numeric(dff['qtd'], errors='coerce')
        dff['volume'] = pd.to_numeric(dff['volume'], errors='coerce')
        dff['volume_ajustado'] = pd.to_numeric(dff['volume_ajustado'], errors='coerce')
        dff['qnt_hoje'] = dff.groupby(['ativo', 'id_cliente'])['qtd'].cumsum()
        
        def calcular_preco_medio(grupo):
            saldo_acoes = 0
            custo_total = 0
            preco_medio = 0  # Inicializa o preço médio
            preco_medio_list = []
            
            for index, row in grupo.iterrows():
                if row['lado'] == 'Compra':
                    saldo_acoes += row['qtd']
                    custo_total += row['volume']
                elif row['lado'] == 'Venda':
                    preco_medio = custo_total / saldo_acoes if saldo_acoes != 0 else preco_medio  # Usa o último preço médio se saldo for zero
                    saldo_acoes += row['qtd']  # Quantidade negativa
                    custo_total += row['qtd'] * preco_medio  # Subtrai do custo total
                
                # Atualiza o preço médio somente se ainda existem ações
                if saldo_acoes != 0:
                    preco_medio = custo_total / saldo_acoes
                preco_medio_list.append(preco_medio)
            
            grupo['preco_medio'] = preco_medio_list
            return grupo

        # Aplicar a função a cada grupo de cliente e ticker
        dff = dff.groupby(['id_cliente', 'ativo']).apply(calcular_preco_medio).reset_index(drop=True)
        
        #Calculando a quantidade acumulada
        dff['qnt_acumulada'] = dff.groupby(['id_cliente', 'ativo'])['qtd'].transform('sum')

        #Identificando que se repete mais uma vez que zero
        grouped = dff.groupby(['id_cliente', 'ativo'])
        for (id_cliente, ativo), group in grouped:
            if (group['qnt_acumulada'] == 0).sum() > 1:
                dff.loc[(dff['id_cliente'] == id_cliente) & (dff['ativo'] == ativo), 'multiple_zero_accumulation'] = 1
            
        # Adiciona a coluna 'multiple_zero_accumulation'
        dff['multiple_zero_accumulation'] = dff.groupby(['id_cliente', 'ativo'])['qnt_acumulada'].transform(lambda x: (x == 0).sum() > 1).astype(int)

        # Calculando o preco executado na operação
        dff['preco_executado'] = abs(dff['volume'] / dff['qtd'])

        # Calculando o ganho financeiro em cada operação
        dff['ganho_financeiro_op'] = abs(dff['qtd'] * dff['preco_executado']) - abs(dff['qtd'] * dff['preco_medio'])

        # Só tem lucro ou prejuízo na Venda
        dff['ganho_financeiro_op'] = np.where(dff['lado'] == 'Compra', 0, dff['ganho_financeiro_op'])

        # Na Operação Geral
        dff['ganho_financeiro'] = dff.groupby(['ativo', 'id_cliente'])['ganho_financeiro_op'].cumsum()

        # Calculando as datas máximas e mínimas
        dff['Data de Entrada'] = dff.groupby(['ativo', 'id_cliente'])['data'].transform('min')
        dff['Data de Saída'] = dff.groupby(['ativo', 'id_cliente'])['data'].transform('max')

        # Puxando o histórico de dividendos
        cursor.execute("SELECT * FROM guelt_main.dividendos")
        plan = cursor.fetchall()
        dividendos = pd.DataFrame(plan, columns=["id", "Ativo", "Valor", "DATA COM",
                                                 "DATA pagamento", "TIPO", "DY"])

        dividendos['DATA COM'] = pd.to_datetime(dividendos['DATA COM'])
        dividendos['Valor'] = pd.to_numeric(dividendos['Valor'], errors='coerce')

        dff['qnt_hoje'] = pd.to_numeric(dff['qnt_hoje'], errors='coerce')
        dff['ativo'] = dff['ativo'].astype(str)
        dividendos['Ativo'] = dividendos['Ativo'].astype(str)

        ### Inicializar a coluna de dividendos recebidos no DataFrame dff
        dff['dividendos_recebidos'] = 0.0

        # Iterar sobre cada linha no DataFrame de dividendos
        for idx, row in dividendos.iterrows():
            ativo = row['Ativo']
            data_com = row['DATA COM']
            valor_dividendo = row['Valor']

            # Filtrar o histórico para encontrar as transações antes da data de corte
            historico_ativo = dff[(dff['ativo'] == ativo) & (dff['data'] <= data_com)]

            # Se não houver histórico antes da data de corte, continuar
            if historico_ativo.empty:
                continue

            # Encontrar a quantidade de ações mais próxima da data de corte
            idx_historico_mais_proximo = historico_ativo.groupby('id_cliente')['data'].idxmax()

            for idx_cliente in idx_historico_mais_proximo:
                qnt_acumulada = dff.loc[idx_cliente, 'qnt_hoje']
                total_dividendos = qnt_acumulada * valor_dividendo

                # Adicionar os dividendos ao valor existente na coluna na linha mais próxima da data de corte
                dff.loc[idx_cliente, 'dividendos_recebidos'] += total_dividendos
                dff.loc[idx_cliente, 'valor_dividendo'] = valor_dividendo

        dff['divi_total'] = dff.groupby(['ativo', 'id_cliente'])['dividendos_recebidos'].cumsum()

        # PEGANDO SÓ OS CLIENTES QUE ZERARAM A OPERAÇÃO
        dff = dff[dff['multiple_zero_accumulation'] == 1]
        dff.reset_index(drop=True, inplace=True)

        # PEGANDO SÓ OS CLIENTES QUE ZERARAM A OPERAÇÃO
        idx = dff.groupby(['id_cliente', 'ativo'])['data'].idxmax()
        dff = dff.loc[idx]

        # ZERANDO DIVIDENDO NEGATIVO
        dff['divi_total'] = np.where(dff['divi_total'] < 0.0, 0.0, dff['divi_total'])

        dff = dff[['id_cliente', 'Data de Entrada', 'ativo', 'produto', 'ganho_financeiro', 'divi_total', 'Data de Saída']]
        dff = dff.sort_values(by='ganho_financeiro', ascending=False)
        
        return dff

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection_pool.putconn(connection)




@login_required
@never_cache
def protected_view(request):
    conta = request.GET.get('client_code', '')  # Definir '' como padrão para 'client_code'
    produto = request.GET.get('produto', 'all')  # Definir 'all' como padrão para 'produto'
    ativo = request.GET.get('ativo', 'all')  # Definir 'all' como padrão para 'ativo'

    data, client_codes, produtos, ativos = get_filtered_data(conta, produto, ativo)
    #data2 = get_filtered_data2(conta, produto, ativo)



    # Verificar se há algum filtro aplicado antes de executar a consulta de operações fechadas
    if conta != '' or ativo != 'all' or produto != 'all':
        data2 = get_filtered_data2(conta, produto, ativo)
    else:
        data2 = pd.DataFrame(columns=['id_cliente', 'Data de Entrada', 'ativo', 'produto', 'ganho_financeiro', 'divi_total', 'Data de Saída'])




    # Calcular valores financeiros
    financeiro_inicial = (data['preco_medio'] * data['qtde']).sum()
    financeiro_hoje = (data['preco_atual'] * data['qtde']).sum()

    # Calcular a variação patrimonial percentual
    variacao_percentual = (financeiro_hoje - financeiro_inicial) / financeiro_inicial * 100 if financeiro_inicial != 0 else 0

    # Calcular ganho financeiro
    data['ganho_financeiro'] = (data['preco_atual'] * data['qtde']) - (data['preco_medio'] * data['qtde'])

    # Calcular rentabilidade, evitando divisão por zero
    def calc_rentabilidade(preco_medio, ganho_financeiro, qtde):
        if preco_medio == 0 or qtde == 0 or pd.isnull(preco_medio):
            return None
        return (ganho_financeiro / (qtde * preco_medio)) * 100

    data['rentabilidade'] = data.apply(lambda row: calc_rentabilidade(row['preco_medio'], row['ganho_financeiro'], row['qtde']), axis=1)

    # Adicionar setas baseadas na rentabilidade
    def format_rentabilidade(x):
        if x is None or pd.isnull(x):
            return 'N/A'
        arrow = '▲' if x > 0 else '▼'
        color = 'green' if x > 0 else 'red'
        return mark_safe(f'<span style="color: {color};">{arrow} {x:,.2f}%</span>')

    data['rentabilidade'] = data['rentabilidade'].apply(format_rentabilidade)

    # Formatando os valores como moeda e percentual
    data['preco_atual'] = data['preco_atual'].apply(lambda x: f"R$ {x:,.2f}")
    data['ganho_financeiro'] = data['ganho_financeiro'].apply(lambda x: f"R$ {x:,.2f}")
    data2['ganho_financeiro'] = data2['ganho_financeiro'].apply(lambda x: f"R$ {x:,.2f}")
    data2['divi_total'] = data2['divi_total'].apply(lambda x: f"R$ {x:,.2f}")

    # Gerar HTML da tabela
    table_html = data.to_html(classes="table table-striped table-hover", index=False, justify='center', escape=False)
    table_html = table_html.replace('<thead>', '<thead class="thead-dark">')

    # Segunda tabela
    table_html2 = data2.to_html(classes="table table-striped table-hover", index=False, justify='center')
    table_html2 = table_html2.replace('<thead>', '<thead class="thead-dark">')

    return render(request, 'myapp/protected.html', {
        'table_html': mark_safe(table_html),
        'table_html2': mark_safe(table_html2),
        'client_codes': client_codes,
        'produtos': produtos,
        'ativos': ativos,
        'financeiro_inicial': f"R$ {financeiro_inicial:,.2f}",
        'financeiro_hoje': f"R$ {financeiro_hoje:,.2f}",
        'variacao_percentual': variacao_percentual,
        'selected_client_code': conta,
        'selected_produto': produto,
        'selected_ativo': ativo
    })

@login_required
@never_cache
def info_view(request):
    return render(request, 'myapp/info.html')


@login_required
@never_cache
def info_view(request):
    return render(request, 'myapp/info.html')