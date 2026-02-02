# Ferramenta de Microdados RAIS

Esta é uma ferramenta de desktop para baixar, processar e importar microdados da Relação Anual de Informações Sociais (RAIS) disponibilizados pelo governo.

## Funcionalidades

-   **Navegação:** Conecta-se ao servidor FTP do Ministério do Trabalho e Previdência para listar os arquivos e anos disponíveis.
-   **Download:** Baixa os arquivos de dados selecionados (formato `.7z`).
-   **Descompressão:** Extrai automaticamente os arquivos de texto (`.txt`) de dentro dos arquivos `.7z`.
-   **Processamento e Importação:**
    -   Lê os arquivos de texto extraídos.
    -   Permite ao usuário selecionar quais colunas de dados deseja importar.
    -   Importa os dados selecionados para um banco de dados SQLite (`data/rais.db`) para fácil acesso e análise. 

## Instalação

1.  Clone este repositório:
    ```bash
    git clone <https://github.com/eltonjmarinho/rais_microdata_tool.git>
    cd rais_microdata_tool
    ```

2.  (Opcional, mas recomendado) Crie e ative um ambiente virtual:
    ```bash
    python -m venv venv
    # No Windows
    venv\Scripts\activate
    # No macOS/Linux
    source venv/bin/activate
    ```

3.  Instale as dependências necessárias:
    ```bash
    pip install -r requirements.txt
    ```

## Uso

Para iniciar a aplicação, execute o seguinte comando na raiz do projeto:

```bash
python gui.py
```

Isso abrirá a janela principal da aplicação, onde você poderá selecionar os anos e arquivos para baixar e processar.
