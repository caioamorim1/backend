export interface CreateParametrosNaoInternacaoDTO {
  nome_enfermeiro?: string;
  numero_coren?: string;
  jornadaSemanalEnfermeiro?: number;
  jornadaSemanalTecnico?: number;
  indiceSegurancaTecnica?: number; // decimal e.g. 0.15
  equipeComRestricao?: boolean;
  diasFuncionamentoMensal?: number;
  diasSemana?: number;
}

export type UpdateParametrosNaoInternacaoDTO = CreateParametrosNaoInternacaoDTO;
