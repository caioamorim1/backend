import { CriarBaselineDTO } from "./baseline.dto";

export interface CreateHospitalDTO {
  nome: string;
  cnpj: string;
  telefone?: string;
  endereco?: string;
  regiaoId?: string;
  baseline?: CriarBaselineDTO;
}

export interface AtualizarHospitalDTO {
  nome?: string;
  cnpj?: string;
  telefone?: string;
  endereco?: string;
  regiaoId?: string;
}
