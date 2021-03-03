package br.com.alst.kafka.vendaIngressos.model;

import java.math.BigDecimal;

public class Venda {
	private Long operacao;
	private Long cliente;
	private Integer qtdeIngressos;
	private BigDecimal total;
	private String status;
	
	
	public Venda() {
		
	}
	
	public Venda(Long operacao, Long cliente, Integer qtdeIngressos, BigDecimal total) {
		super();
		this.operacao = operacao;
		this.cliente = cliente;
		this.qtdeIngressos = qtdeIngressos;
		this.total = total;
	}

	public Long getOperacao() {
		return operacao;
	}

	public void setOperacao(Long operacao) {
		this.operacao = operacao;
	}

	public Long getCliente() {
		return cliente;
	}

	public void setCliente(Long cliente) {
		this.cliente = cliente;
	}

	public Integer getQtdeIngressos() {
		return qtdeIngressos;
	}

	public void setQtdeIngressos(Integer qtdeIngressos) {
		this.qtdeIngressos = qtdeIngressos;
	}

	public BigDecimal getTotal() {
		return total;
	}

	public void setTotal(BigDecimal total) {
		this.total = total;
	}
	
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public String toString() {
		return "Venda [operacao=" + operacao + ", cliente=" + cliente + ", qtdeIngressos=" + qtdeIngressos + ", total="
				+ total + ", status=" + status +"]";
	}
	
	
	
}
