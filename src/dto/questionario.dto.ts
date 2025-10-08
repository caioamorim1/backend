// src/dtos/questionario.dto.ts
export interface PerguntaDTO {
  categoria: string;
  texto: string;
  tipoResposta: "sim_nao_na" | "texto" | "numero" | "data" | "multipla_escolha";
  opcoes?: string[];
  obrigatoria: boolean;
}

export interface CreateQuestionarioDTO {
  nome: string;
  perguntas: PerguntaDTO[];
}

export interface UpdateQuestionarioDTO {
  nome?: string;
  perguntas?: PerguntaDTO[];
}

// DTO para listagem de questionários (com paginação/filtro)
export interface ListQuestionarioDTO {
  nome?: string;
  page?: number;
  limit?: number;
}

// DTO para retorno de questionário (ex.: resposta da API)
export interface QuestionarioResponseDTO {
  nome: string;
  perguntas: PerguntaDTO[];
  created_at: Date;
  updated_at: Date;
}
