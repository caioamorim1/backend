import { Cargo } from "../entities/Cargo";

export interface CreateCargoDTO {
  nome: string;
  hospitalId: string;
  salario?: string;
  carga_horaria?: string;
  descricao?: string;
  adicionais_tributos?: string;
}

export interface UpdateCargoDTO {
  nome?: string;
  salario?: string;
  carga_horaria?: string;
  descricao?: string;
  adicionais_tributos?: string;
}
