export interface CreateRegiaoDTO {
  nome: string;
  grupoId?: string;
}

export interface AtualizarRegiaoDTO {
  nome?: string;
  grupoId?: string | null;
}
