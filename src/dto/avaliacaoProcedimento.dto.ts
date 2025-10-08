export interface AvaliacaoProcedimentoDTO {
  id: string;
  dataAplicacao: string;
  unidadeId?: string;
  sitioFuncionalId?: string;
  responsavelId?: string;
  tipo_procedimento: string;
  paciente_identificacao?: string;
  inicio_previsto?: Date;
  inicio_real?: Date;
  fim_previsto?: Date;
  fim_real?: Date;
  classificacao: string;
  status: string;
  recursos_utilizados?: any;
  duracao_minutos?: number;
  observacoes?: string;
}

export interface CriarAvaliacaoProcedimentoDTO {
  dataAplicacao: string;
  unidadeId: string;
  sitioFuncionalId: string;
  responsavelId?: string;
  tipo_procedimento: string;
  paciente_identificacao?: string;
  inicio_previsto?: Date;
  fim_previsto?: Date;
  classificacao: string;
  recursos_utilizados?: any;
  observacoes?: string;
}

export interface IniciarProcedimentoDTO {
  inicio_real: Date;
  recursos_utilizados?: any;
}

export interface FinalizarProcedimentoDTO {
  fim_real: Date;
  observacoes?: string;
}
