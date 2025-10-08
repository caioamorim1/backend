export interface CreateLeitoDTO {
  unidadeId: string;
  numero: string;
}

export interface UpdateLeitoStatusDTO {
  status: string;
  justificativa?: string | null;
}
