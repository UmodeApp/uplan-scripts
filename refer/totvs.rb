class ColmeiaUplan::Totvs
  include EnviromentHelper

  attr_reader :entity

  def initialize(entity)
    @entity = entity
  end

  def oracle_db
    return @oracle_db if @oracle_db.present?

    @oracle_db =
      OCI8.new(
        env_fetch("colmeia_oracle_username"),
        env_fetch("colmeia_oracle_password"),
        env_fetch("colmeia_oracle_dbname")
      )
    @oracle_db.exec("ALTER session SET NLS_DATE_FORMAT='DD/MM/YY'")
    @oracle_db
  end

  def fetch_vendas(start_date, end_date)
    # Seleciona todas as vendas atualizadas/realizadas dentro dos intervalos de start_date e end_date.
    # Apenas vendas que foram realizadas no mês e ano atual.
    fetch_table(
      "SELECT * FROM UCOLMEIA.CR_UROCKET_VENDAS WHERE ((DT_ATUALIZACAO BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')) OR (DT_TRANSACAO BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD'))) AND EXTRACT(MONTH FROM DT_TRANSACAO) = EXTRACT(MONTH FROM SYSDATE) AND EXTRACT(YEAR FROM DT_TRANSACAO) = EXTRACT(YEAR FROM SYSDATE)",
      start_date,
      end_date
    )
  end

  def fetch_all_lojas
    fetch_table("SELECT * FROM UCOLMEIA.CR_UROCKET_LOJAS")
  end

  def fetch_clientes(start_date, end_date)
    fetch_table(
      "SELECT * FROM UCOLMEIA.CR_UROCKET_CLIENTES WHERE DATA_ATUALIZACAO BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')",
      start_date,
      end_date
    )
  end

  # Colmeia tem os dados de stokes apenas do dia atual
  def fetch_stock(start_date, end_date)
    # Converte as datas para DateTime e adiciona um dia a cada extremidade
    start_date_with_buffer = (DateTime.parse(start_date) - 1).strftime('%Y-%m-%d %H:%M:%S')
    end_date_with_buffer = (DateTime.parse(end_date) + 1).strftime('%Y-%m-%d %H:%M:%S')
  
    fetch_table(
      "SELECT * FROM UCOLMEIA.CR_UROCKET_ESTOQUE WHERE DT_ESTOQUE BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE(:end_date, 'YYYY-MM-DD HH24:MI:SS')",
      start_date_with_buffer,
      end_date_with_buffer
    )
  end

  def fetch_all_clientes
    fetch_table("SELECT * FROM UCOLMEIA.CR_UROCKET_CLIENTES")
  end

  def fetch_all_produtos
    fetch_table("SELECT * FROM UCOLMEIA.CR_UROCKET_PRODUTOS")
  end

  def fetch_produtos(start_date, end_date)
    fetch_table(
      "SELECT * FROM UCOLMEIA.CR_UROCKET_PRODUTOS WHERE MAX_DATE BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')",
      start_date,
      end_date
    )
  end

  def sum_vendas(cd_vendedor, start_date, end_date, use_cache = false)
    fetch_table(
      "
        select sum(vl_compra) from
        UCOLMEIA.CR_UROCKET_VENDAS
        where
        cd_vendedor = :cd_vendedor
        AND dt_transacao >= :start_date
        AND dt_transacao <= :end_date
        AND TP_SITUACAO = 'ATENDIDA'
      ",
      cd_vendedor,
      start_date,
      end_date,
      use_cache
    )
  end

  def fetch_metas(start_date, end_date)
    fetch_table(
      "SELECT * FROM UCOLMEIA.CR_UROCKET_METAS WHERE DT_CADASTRO BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')",
      start_date,
      end_date
    )
  end

  def fetch_vendedores(start_date, end_date)
    fetch_table(
      "SELECT * FROM UCOLMEIA.CR_UROCKET_VENDEDOR WHERE DT_ATUALIZACAO BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')",
      start_date,
      end_date
    )
  end

  protected

  # buscar no banco de dados do totvs (oracle) os dados do sql do período
  # este método guarda um cache de todas as chamadas sql.
  def fetch_table(sql, start_date = nil, end_date = nil)
    # Substitua as datas no SQL apenas se elas forem fornecidas
    query = sql
    query = query.gsub(":start_date", start_date) if start_date
    query = query.gsub(":end_date", end_date) if end_date

    Rails.logger.debug("[ColmeiaUplan::Totvs] Query from oracle: #{query}")

    cached_result =
      Etl::ColmeiaTotvs.where(entity: entity, query: query).first_or_initialize
    return cached_result if cached_result.persisted?

    Rails.logger.info(
      "[ColmeiaUplan::Totvs] Cached results not found, get from oracle"
    )

    # Execute a consulta com ou sem as datas
    cursor =
      if start_date && end_date
        oracle_db.exec(sql, start_date, end_date)
      else
        oracle_db.exec(sql)
      end

    csv_data =
      CSV.generate(headers: true) do |csv|
        csv << cursor.get_col_names

        while res = cursor.fetch
          csv << res.to_a
        end
      end

    cached_result.results_csv = csv_data
    cached_result.save!

    cached_result
  end
end
