module ColmeiaUplan::ProcessVendas
  STATUS_MAPPING = { "ATENDIDA" => :invoiced, "CANCELADA" => :canceled }.freeze

  def process_vendas(start_date = Date.yesterday, end_date = Date.yesterday)
    vendas =
      ColmeiaUplan::Totvs.new(entity).fetch_vendas(
        start_date.to_s,
        end_date.to_s
      )

    Rails.logger.info(
      "[ColmeiaUplan::ProcessVendas] Processing #{vendas.results.to_a.size} orders"
    )

    vendas.results.each_with_index do |venda, index|
      Rails.logger.info(
        "[ColmeiaUplan::ProcessVendas] Processing order #{index + 1}: #{venda.to_h.values.join(", ")}"
      )
      Order.transaction do
        begin
          process_order(venda)
        rescue StandardError => e
          Rails.logger.error(
            "[Colmeia::ProcessVendas] Error processing order: #{e.message}\n\n#{e.backtrace.join("\n")}"
          )

          raise e
        end
      end
    end

    vendas.update!(processed: true)
  end

  protected

  def process_order(venda)
    number = generate_order_number(venda)
    order = Order.where(entity: entity, number: number).first_or_initialize
    store_venda = find_store(venda)
    if order.new_record?
      order.store = store_venda
      order.seller = find_seller(venda, store_venda)
      order.organization = find_organization(venda)
      order.attributes = {
        ordered_at: Time.find_zone("UTC").parse(venda["DT_TRANSACAO"]).utc,
        status: STATUS_MAPPING[venda["TP_SITUACAO"]],
        attrs: venda.to_h
      }
      order.save!
    end

    process_order_item(order, venda)
  end

  def process_order_item(order, venda)
    integration_id = generate_order_item_number(venda)
    order_item =
      order
        .order_items
        .where(integration_id: integration_id)
        .first_or_initialize

    order_item.product = find_product(venda)
    order_item.quantity_ordered = venda["QT_VENDIDA"].to_i
    order_item.quantity_invoiced = venda["QT_VENDIDA"].to_i
    order_item.total_ordered = venda["QT_VENDIDA"].to_i
    order_item.total_invoiced = venda["QT_VENDIDA"].to_i
    order_item.current_price_total = venda["VL_COMPRA"].to_f
    order_item.attributes = {
      discount_total: venda["PR_DESCONTO"].to_f,
      attrs: venda.to_h
    }

    order_item.save!
  end

  def find_product(venda)
    product =
      Product.where(
        entity: entity,
        sku: venda["CD_PRODUTO"],
        size: venda["DS_TAM"],
        color: venda["DS_COR"],
        integration_id: venda["CD_PRODUTO"]
      ).first_or_initialize

    if product.new_record?
      product.name = venda["DESCR"]
      product.attrs = {
        ORIGINAL_PRICE: venda["PREC_ORIGINAL"],
        CLASSE: venda["CLASSE"],
        SUBCLASSE: venda["SUB_CLASSE"],
        COLECTION: venda["ANO"],
        FABRIC: venda["TECIDO"],
        PERSONA: venda["PERSONA"]
      }
      product.save!
    end

    product
  end

  def generate_order_item_number(venda)
    [
      venda["NR_TRANSACAO"],
      venda["CD_PRODUTO"],
      venda["DS_COR"],
      venda["DS_TAM"],
      venda["NR_ITEM"]
    ].join("-")
  end

  def find_store(venda)
    return nil unless venda["LOJA"].present?

    Store.where(entity: entity, name: venda["LOJA"]).first_or_create!
  end

  def find_seller(venda, store_venda)
    seller =
      Seller.where(integration_id: venda["CD_VENDEDOR"]).first_or_initialize

    if seller.new_record?
      seller.store_id = store_venda.id
      seller.name = venda["NM_VENDEDOR"]
      seller.attrs = {
        DT_ATUALIZACAO: venda["DT_ATUALIZACAO"],
        LOJA_NAME: venda["CD_LOJA"]
      }
      seller.save!
    end

    seller
  end

  def find_organization(venda)
    org =
      Organization.where(
        entity: entity,
        integration_id: venda["CD_CLIENTE"]
      ).first_or_initialize

    if org.new_record?
      org.name = venda["NM_CLIENTE"]
      org.registration_number = venda["CD_CLIENTE"]
      org.save!
    end

    org
  end

  def generate_order_number(venda)
    venda["NR_TRANSACAO"]
  end
end
