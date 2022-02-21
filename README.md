# PostGres Data Transfer

## Descrição

Projeto realizado de forma a transferir os dados de 2 bancos de dados em hosts diferentes para um terceiro. Toda a aplicação foi feita com base em bancos de dados PostGres e o código em Python.

## Variáveis de Ambiente

O acesso aos bancos de dados é feito a partir de variáveis de ambiente, sendo elas :

- HOST_A : Host Banco de dados 1
- HOST_B : Host Banco de dados 2
- DB : Nome do banco de dados 1 e 2 associados
- PSTG_USER : Usuário de acesso dos bancos de dados 1 e 2
- PSTG_PSW : Senha de acesso dos bancos de dados 1 e 2
- HOST_C : Host Banco de dados onde os dados serão colocados
- DB_C : Nome do banco de dados 3
- PSTG_USER_C : Nome de usuário do banco de dados 3
- PSTG_PSW_C : Senha de acesso ao banco de dados 3
- PORT_A : Porta do banco 1
- PORT_B : Porta do banco 2
- PORT_C : Porta do banco 3
- TIMER : Horario para execução do loop

De forma a melhor executar o código se é aconselhado criar um arquivo .env com todas as variáveis necessárias.

## Rodando o Sistema sem Containers

### Clone o repositório para utilização

```bash
$ git clone https://github.com/luanjosecar/DataTransfer.git
```

### Acesse a pasta

```bash
$ cd DataTransfer
```

### Instale as dependências

```bash
$ pip install -r requirements.txt
```

### Executando a aplicação

```bash
$ python src/main.py
```

## Rodando em Docker

Com o docker e o docker-compose instalado se é necessário apenas indicar as variáveis no sistema a partir do arquivo docker-compose.yaml, ou criar um arquivo .env onde elas estejam contidas.
Para o caso do arquivo .env criado basta rodar a linha abaixo na pasta de diretório principal.

```bash
$ docker-compose --env-file .env up
```

## Execução de testes

Para a execução do ambiente em teste basta apenas criar as variáveis do banco de dados no qual serão armazenadas as informações e rodar o arquivo docker-compose dentro da pasta test

```bash
$ docker-compose up
```
