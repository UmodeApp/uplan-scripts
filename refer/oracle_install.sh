# Baixe o arquivo zip do Oracle Instant Client
wget https://umode-content.s3.us-east-2.amazonaws.com/docker/instantclient_12_2.zip

# Crie um diretório para o Oracle Instant Client
mkdir -p ~/oracle

# Descompacte o arquivo zip para o diretório criado
unzip instantclient_12_2.zip -d ~/oracle

# Adicione o caminho do Oracle Instant Client ao LD_LIBRARY_PATH
echo 'export LD_LIBRARY_PATH=~/oracle/instantclient_12_2:$LD_LIBRARY_PATH' >> ~/.bashrc

# Recarregue o arquivo .bashrc para aplicar as mudanças
source ~/.bashrc

sudo apt-get update
sudo apt-get install libaio1
