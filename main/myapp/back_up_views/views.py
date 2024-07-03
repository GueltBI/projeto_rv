from django.shortcuts import render, redirect, resolve_url
from django.contrib.auth import login, authenticate, logout
from .forms import RegisterForm, LoginForm
from django.views.decorators.cache import never_cache
from django.contrib.auth.decorators import login_required
import pandas as pd
import locale
from django.utils.safestring import mark_safe


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
    return redirect('home')




########################## FUNÇÕES DE PRIMEIRA PÁGINA ######################################


# Função para ler e filtrar dados do Excel
def get_filtered_data(conta=None):
    
    # Carregar o arquivo Excel
    file_path = 'C:\\Users\\rafael.ciarelli\\Downloads\\data (2).xlsx'  # Atualize com o caminho real do seu arquivo
    file_path2 = 'C:\\Users\\rafael.ciarelli\\Downloads\\data (30).xlsx'  # Atualize com o caminho real do seu arquivo
    #file_path3 = 'C:\\Users\\rafael.ciarelli\\Downloads\\cotacao.xlsx'  # Atualize com o caminho real do seu arquivo

    #FAZENDO OS CALCULOS
    carteira = pd.read_excel(file_path)
    proventos = pd.read_excel(file_path2)
    #cotacao = pd.read_excel(file_path3)


    
    proventos = proventos.groupby(['Cliente', 'Papel'])['Valor Provisionado'].sum().reset_index()

    proventos['Cliente'] = proventos['Cliente'].astype(str).str.rstrip('.0')
    proventos['Cliente'] = proventos['Cliente'].astype(str)
    proventos['Papel'] = proventos['Papel'].astype(str)
    carteira['Ativo'] = carteira['Ativo'].astype(str)
    carteira['Conta'] = carteira['Conta'].astype(str)
    #cotacao['Ticker'] = cotacao['Ticker'].astype(str)




    carteira=carteira[['Conta','Ativo','Qtde','AUC','Preço Médio','Produto']]
    carteira['Financeiro Entrada'] = carteira['Qtde'] * carteira['Preço Médio']

    carteira = pd.merge(carteira, proventos[['Cliente', 'Papel', 'Valor Provisionado']], left_on=['Conta', 'Ativo'], right_on=['Cliente', 'Papel'], how='left')
    #carteira = pd.merge(carteira, cotacao[['Ticker', 'Latest Price']], left_on=['Ativo'], right_on=['Ticker'], how='left')


    carteira=carteira[['Conta','Ativo','Qtde','AUC','Preço Médio','Produto','Financeiro Entrada','Valor Provisionado','Latest Price']]
    carteira = carteira.fillna(0)
    carteira = carteira.rename(columns={'Valor Provisionado': 'Proventos'})

    # Obter todos os códigos de cliente
    client_codes = carteira['Conta'].unique()

    # Filtrar os dados pela conta, se fornecida
    if conta:
        carteira = carteira[carteira['Conta'] == str(conta)]  # Converter para int se necessário

    return carteira, client_codes


locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')


@login_required
@never_cache
def protected_view(request):
    conta = request.GET.get('client_code')
    produto = request.GET.get('produto', 'all')  # Obtém o produto da requisição

    data, client_codes = get_filtered_data(conta)


    # Obter lista de produtos únicos para o filtro
    produtos = data['Produto'].unique() if not data.empty else []

    # Filtrar os dados pela conta e produto se fornecido
    if produto != 'all':
        data = data[data['Produto'] == produto]

    if not data.empty:
        financeiro_inicial = (data['Qtde'] * data['Preço Médio']).sum()
        financeiro_inicial_porativo= data['Qtde'] * data['Preço Médio']

        Proventos = data['Proventos'].sum()
        financeiro_hoje = (data['Latest Price'] * data['Qtde']).sum()
        financeiro_hoje_porativo = data['Latest Price'] * data['Qtde']
        data['Financeiro Hoje'] = data['Latest Price'] * data['Qtde']



        data['Variação Cota'] = (financeiro_hoje_porativo - financeiro_inicial_porativo) / financeiro_inicial_porativo

        if financeiro_inicial != 0:
            variacao_percentual = ((financeiro_hoje - financeiro_inicial) / financeiro_inicial) * 100
        else:
            variacao_percentual = 0

        data['Seta'] = data['Variação Cota'].apply(lambda x: '▲' if x >= 0 else '▼')

        data['Variação Cota'] = data.apply(
            lambda row: f"{row['Variação Cota']*100:.2f}% <span style='color:green;'>{row['Seta']}</span>"
            if row['Variação Cota'] >= 0
            else f"{row['Variação Cota']*100:.2f}% <span style='color:red;'>{row['Seta']}</span>", axis=1)

        # Remove a coluna 'Seta' após seu uso
        data.drop(columns=['Seta'], inplace=True)

    else:
        financeiro_inicial = 0
        financeiro_hoje = 0
        variacao_percentual = 0
        Proventos = 0

    financeiro_inicial = locale.currency(financeiro_inicial, grouping=True)
    financeiro_hoje = locale.currency(financeiro_hoje, grouping=True)
    Proventos = locale.currency(Proventos, grouping=True)

    data['Preço Médio'] = data['Preço Médio'].apply(lambda x: locale.currency(x, grouping=True))
    data['AUC'] = data['AUC'].apply(lambda x: locale.currency(x, grouping=True))
    
    data['Proventos'] = data['Proventos'].apply(lambda x: locale.currency(x, grouping=True))

    data = data.sort_values(by='Variação Cota', ascending=False)

    table_html = data.to_html(classes='table table-striped table-bordered', index=False, float_format='{:,.2f}'.format, escape=False)
    
    produtos = data['Produto'].unique()  # Obtém a lista de produtos únicos


    return render(request, 'myapp/protected.html', {
        'table_html': mark_safe(table_html),
        'client_codes': client_codes,
        'produtos': produtos,
        'financeiro_inicial': financeiro_inicial,
        'financeiro_hoje': financeiro_hoje,
        'variacao_percentual': variacao_percentual,
        'Proventos': Proventos,

    })


@login_required
@never_cache
def info_view(request):
    return render(request, 'myapp/info.html')