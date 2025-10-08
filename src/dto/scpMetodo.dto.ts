import { ClassificacaoCuidado } from "../entities/AvaliacaoSCP";

export interface ScpOptionDTO {
  label: string;
  value: number;
}

export interface ScpQuestionDTO {
  key: string;
  text: string;
  options: ScpOptionDTO[];
}

export interface ScpFaixaDTO {
  min: number;
  max: number;
  classe: ClassificacaoCuidado;
}

export interface CreateScpMetodoDTO {
  key: string; // ex.: FUGULIN, PERROCA, DINI, OUTRO
  title: string;
  description?: string;
  questions: ScpQuestionDTO[];
  faixas: ScpFaixaDTO[];
}

export interface UpdateScpMetodoDTO extends Partial<CreateScpMetodoDTO> {}
