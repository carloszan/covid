from functools import wraps
import logging


def registrar_execucao(fn):
    """
    Decorator que registra o início, conclusão e possíveis erros da execução de uma função.

    Registra mensagens de log com os seguintes eventos:
    - INFO quando a função inicia
    - INFO quando a função é concluída com sucesso
    - ERROR quando ocorre uma exceção

    Parâmetros
    ----------
    fn : callable
        Função a ser decorada e monitorada

    Retorna
    -------
    wrapper : callable
        Função decorada que inclui o registro de log

    Exceções
    --------
    Relança qualquer exceção ocorrida na função original após registrar no log

    Exemplos
    --------
    >>> @registrar_execucao
    ... def processar_dados(dados):
    ...     return [x * 2 for x in dados]

    >>> @registrar_execucao
    ... def operacao_critica():
    ...     # Código importante
    ...     pass

    Notas
    -----
    - Requer configuração prévia do módulo logging
    - Preserva os metadados da função original (__name__, __doc__, etc)
    - O log inclui automaticamente o nome da função executada

    Configuração Recomendada
    -----------------------
    >>> import logging
    >>> logging.basicConfig(
    ...     level=logging.INFO,
    ...     format='%(asctime)s - %(levelname)s - %(message)s'
    ... )
    """
    @wraps(fn)  # Preserva os metadados da função original
    def wrapper(*args, **kwargs):
        logging.info(f"Iniciando processo: {fn.__name__}")

        try:
            resultado = fn(*args, **kwargs)
            logging.info(f"Processo concluído com sucesso: {fn.__name__}")
            return resultado

        except Exception as e:
            logging.error(
                f"Erro no processo {fn.__name__}: {str(e)}", exc_info=True)
            raise

    return wrapper
