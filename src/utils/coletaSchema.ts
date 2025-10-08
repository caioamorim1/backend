// src/schemas/coletaSchema.ts

export interface PerguntaSchema {
  categoria: string;
  texto: string;
  tipoResposta: "sim_nao_na" | "texto" | "numero" | "data" | "multipla_escolha";
  obrigatoria: boolean;
  opcoes?: string[];
}

export interface QuestionarioSchema {
  nome: string; // nome do questionário
  perguntas: PerguntaSchema[];
}

export const coletaSchema: QuestionarioSchema[] = [
  {
    nome: "INTERAÇÃO DE PROCESSOS",
    perguntas: [
      {
        categoria: "INTERAÇÃO DE PROCESSOS",
        texto:
          "Interação de processo definida e executada conforme requisitos com CME?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "INTERAÇÃO DE PROCESSOS",
        texto:
          "Interação de processo definida e executada conforme requisitos com Farmácia?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "INTERAÇÃO DE PROCESSOS",
        texto:
          "Interação de processo definida e executada conforme requisitos com Nutrição?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "INTERAÇÃO DE PROCESSOS",
        texto:
          "Interação de processo definida e executada conforme requisitos com Abastecimento Logístico?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "INTERAÇÃO DE PROCESSOS",
        texto:
          "Interação de processo definida e executada conforme requisitos com Rouparia?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "INTERAÇÃO DE PROCESSOS",
        texto:
          "Interação de processo definida e executada conforme requisitos com Higienização?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "INTERAÇÃO DE PROCESSOS",
        texto:
          "Interação de processo definida e executada conforme requisitos com equipe de recepção?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
    ],
  },
  {
    nome: "GESTÃO DE ENFERMAGEM",
    perguntas: [
      {
        categoria: "GESTÃO DE ENFERMAGEM",
        texto: "Existe um modelo assistencial implementado?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "GESTÃO DE ENFERMAGEM",
        texto: "Existe sistema de classificação de pacientes ?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
    ],
  },
  {
    nome: "PROCESSOS E ROTINAS",
    perguntas: [
      {
        categoria: "PROCESSOS E ROTINAS",
        texto: "Atendimento 24 horas/dia, 7 dias/semana?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "PROCESSOS E ROTINAS",
        texto:
          "É Serviço de referência (outros hospitais ou de linhas de cuidado)?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "PROCESSOS E ROTINAS",
        texto: "É a única porta de entrada do hospital?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
    ],
  },
  {
    nome: "ATIVIDADES DE SUPORTE",
    perguntas: [
      {
        categoria: "ATIVIDADES DE SUPORTE",
        texto: "Existe profissional administrativo de posto ?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "ATIVIDADES DE SUPORTE",
        texto: "Existe atuação de maqueiro no setor ?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "ATIVIDADES DE SUPORTE",
        texto: "Existe farmácia satélite integrada ao PA ?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "ATIVIDADES DE SUPORTE",
        texto: "Existe padrão para prescrição de cuidados ?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
    ],
  },
  {
    nome: "GESTÃO DE PESSOAS",
    perguntas: [
      {
        categoria: "GESTÃO DE PESSOAS",
        texto: "O indicador de Turnonver é gerenciado ?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "GESTÃO DE PESSOAS",
        texto: "O indicador de Absenteísmo é gerenciado?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "GESTÃO DE PESSOAS",
        texto: "Existe equipe específica e completa para cobertura de férias ?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "GESTÃO DE PESSOAS",
        texto: "Existe programa de educação continuada em curso/implantada ?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
    ],
  },
  {
    nome: "EQUIPE MÉDICA",
    perguntas: [
      {
        categoria: "EQUIPE MÉDICA",
        texto:
          "Existe equipe médica dedicada em tempo integral de funcionamento ?",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
      {
        categoria: "EQUIPE MÉDICA",
        texto:
          "A enfermagem realiza alguma atividade que deveria ser realizado pela equipe médica",
        tipoResposta: "sim_nao_na",
        obrigatoria: true,
      },
    ],
  },
];
