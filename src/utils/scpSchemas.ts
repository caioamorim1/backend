export enum SCPType {
  FUGULIN = "FUGULIN",
  PERROCA = "PERROCA",
  DINI = "DINI",
}

export interface Option {
  label: string;
  value: number;
}

export interface Question {
  key: string; // chave que será usada no objeto `itens` na avaliação
  text: string;
  options: Option[];
}

export interface SCPSchema {
  scp: SCPType;
  title: string;
  description?: string;
  questions: Question[];
}

// Esquemas iniciais. Ajuste as perguntas/valores conforme o protocolo oficial do hospital.
export const scpSchemas: Record<SCPType, SCPSchema> = {
  FUGULIN: {
    scp: SCPType.FUGULIN,
    title: "Fugulin",
    description: "Fugulin — itens e pontuações (4 a 1).",
    questions: [
      {
        key: "estado_mental",
        text: "Estado mental",
        options: [
          { label: "Inconsciente", value: 4 },
          { label: "Períodos de inconsciência", value: 3 },
          {
            label: "Períodos de desorientação no espaço e no tempo",
            value: 2,
          },
          { label: "Orientação no tempo e espaço", value: 1 },
        ],
      },
      {
        key: "oxigenacao",
        text: "Oxigenação",
        options: [
          { label: "Ventilação Mecânica", value: 4 },
          {
            label: "Uso contínuo de máscara ou cateter de O2",
            value: 3,
          },
          {
            label: "Uso intermitente de máscara ou cateter de O2",
            value: 2,
          },
          { label: "Não dependente de oxigenioterapia", value: 1 },
        ],
      },
      {
        key: "sinais_vitais",
        text: "Sinais vitais",
        options: [
          {
            label: "Controle em intervalos menores ou igual a 02h",
            value: 4,
          },
          { label: "Controle em intervalos de 04h", value: 3 },
          { label: "Controle em intervalos de 06h", value: 2 },
          {
            label: "Controle de rotina - intervalo de 8h",
            value: 1,
          },
        ],
      },
      {
        key: "motilidade",
        text: "Motilidade",
        options: [
          {
            label:
              "Incapaz de movimentar qualquer segmento corporal. Mudança de decúbito emovimentação passiva programada pela enfermagem",
            value: 4,
          },
          {
            label:
              "Dificuldade para movimentar segmentos corporais. Mudança de decúbito emovimentação passiva auxiliada pela enfermagem",
            value: 3,
          },
          { label: "Limitação de movimentos", value: 2 },
          {
            label: "Movimenta todos os segmentos corporais",
            value: 1,
          },
        ],
      },
      {
        key: "deambulacao",
        text: "Deambulação",
        options: [
          { label: "Restrito ao leito", value: 4 },
          {
            label: "Locomoção através de cadeira de rodas",
            value: 3,
          },
          {
            label: "Necessita de auxílio para deambular",
            value: 2,
          },
          { label: "Deambula sozinho", value: 1 },
        ],
      },
      {
        key: "alimentacao",
        text: "Alimentação",
        options: [
          { label: "Através de cateter central", value: 4 },
          { label: "Através de cateter enteral", value: 3 },
          { label: "Via oral com auxílio", value: 2 },
          { label: "Autossuficiente", value: 1 },
        ],
      },
      {
        key: "cuidado_corporal",
        text: "Cuidado corporal",
        options: [
          {
            label: "Banho no leito pela equipe de enfermagem",
            value: 4,
          },
          {
            label: "Banho no chuveiro e higiene oral pela equipe de enfermagem",
            value: 3,
          },
          {
            label:
              "Banho no chuveiro e higiene oral auxiliados pela equipe de enfermagem",
            value: 2,
          },
          { label: "Autossuficiente", value: 1 },
        ],
      },
      {
        key: "eliminacao",
        text: "Eliminação",
        options: [
          {
            label: "Evacuação no leito e/ou uso de cateter vesical de demora",
            value: 4,
          },
          {
            label: "Uso de comadre ou eliminação no leito com troca de fraldas",
            value: 3,
          },
          { label: "Uso de vaso sanitário com auxílio", value: 2 },
          { label: "Autossuficiente", value: 1 },
        ],
      },
      {
        key: "terapeutica",
        text: "Terapêutica",
        options: [
          { label: "Uso de drogas vasoativas", value: 4 },
          { label: "EV contínua ou através de cateter", value: 3 },
          { label: "EV intermitente", value: 2 },
          { label: "IM ou VO", value: 1 },
        ],
      },
      {
        key: "integridade_cutaneo_mucosa",
        text: "Integridade cutâneo-mucosa",
        options: [
          {
            label:
              "Presença de solução de continuidade com destruição da derme, epiderme, músculo e comprometimento das demais estruturas de suporte como tendões. Eviceração.",
            value: 4,
          },
          {
            label:
              "Presença de solução de continuidade da pele envolvendo tecido subcutâneo e músculo. Incisões cirúrgicas. Ostomias e drenos",
            value: 3,
          },
          {
            label:
              "Presença de alteração de cor da pele (equimose/hiperemia) ou solução de continuidade da pele envolvendo a epiderme, derme ou ambas.",
            value: 2,
          },
          { label: "Pele íntegra", value: 1 },
        ],
      },
      {
        key: "curativo",
        text: "Curativo",
        options: [
          {
            label: "Troca de curativo 03 vezes ao dia ou mais",
            value: 4,
          },
          {
            label: "Troca de curativo até 02 vezes ao dia",
            value: 3,
          },
          { label: "Troca de curativo 01 vez ao dia", value: 2 },
          { label: "Sem curativo", value: 1 },
        ],
      },
      {
        key: "tempo_troca_curativo",
        text: "Tempo na troca do curativo",
        options: [
          { label: "Superior a 30 minutos", value: 4 },
          { label: "Entre 15 e 30 minutos", value: 3 },
          { label: "Entre 05 a 15 minutos", value: 2 },
          { label: "Sem curativo ou limpeza da ferida", value: 1 },
        ],
      },
    ],
  },
  PERROCA: {
    scp: SCPType.PERROCA,
    title: "Perroca",
    description: "Escala Perroca completa (13 itens, opções 1..5).",
    questions: [
      {
        key: "estado_mental",
        text: "1 - Estado Mental e Nível de Consciência (habilidade em manter a percepção e as atividades cognitivas)",
        options: [
          {
            label:
              "Acordado; interpretação precisa de ambiente e tempo; executa, sempre, corretamente ordens verbalizadas;  preservação da memória .",
            value: 1,
          },
          {
            label:
              "Acordado; interpretação precisa de ambiente e tempo; segue instruções corretamente apenas algumas vezes; dificuldade de memória.",
            value: 2,
          },
          {
            label:
              "Acordado; interpretação imprecisa de ambiente e tempo em alguns momentos; dificilmente segue instruções corretamente; dificuldade aumentada de memória.",
            value: 3,
          },
          {
            label:
              "Acordado; interpretação imprecisa de ambiente e tempo em todos os momentos; não segue instruções Corretamente; perda de memória.",
            value: 4,
          },
          {
            label:
              "Desacordado; ausência de resposta verbal e manutenção de respostas à estímulos dolorosos ou ausência de respostas verbais e motoras.",
            value: 5,
          },
        ],
      },
      {
        key: "oxigenacao",
        text: "2 - Oxigenação (aptidão em manter a permeabilidade das vias aéreas e o equilíbrio nas trocas gasosas por si mesmo, com auxílio da equipe de enfermagem e/ou de equipamentos)",
        options: [
          { label: "Não requer oxigenoterapia.", value: 1 },
          {
            label:
              "Requer uso intermitente ou contínuo de oxigênio sem necessidade de desobstrução de vias aéreas.",
            value: 2,
          },
          {
            label:
              "Requer uso intermitente ou contínuo de oxigênio com necessidade de desobstrução de vias aéreas.",
            value: 3,
          },
          {
            label:
              "Requer uso de oxigênio por traqueostomia ou tubo orotraqueal.",
            value: 4,
          },
          { label: "Requer ventilação mecânica.", value: 5 },
        ],
      },
      {
        key: "sinais_vitais",
        text: "3 - Sinais Vitais (necessidade de observação e de controle dos parâmetros vitais: temperatura corporal, pulso, padrão respiratório, saturação de oxigênio e pressão arterial, arterial média e venosa central)",
        options: [
          {
            label: "Requer controle de sinais vitais em intervalos de 6 horas.",
            value: 1,
          },
          { label: "Requer controle em intervalos de 4 horas.", value: 2 },
          { label: "Requer controle em intervalos de 2 horas.", value: 3 },
          {
            label:
              "Requer controle de sinais vitais em intervalos menores do que 2 horas.",
            value: 4,
          },
          {
            label:
              "Requer controle de sinais vitais em intervalos menores do que 2 horas e controle de pressão arterial média e/ou pressão venosa central e/ou saturação de oxigênio.",
            value: 5,
          },
        ],
      },
      {
        key: "nutricao_hidratacao",
        text: "4 - Nutrição e Hidratação (habilidade de ingerir nutrientes e líquidos para atender às necessidades metabólicas, por si mesmo, com auxílio de acompanhantes ou da equipe de enfermagem ou por meio de sondas e cateteres)",
        options: [
          { label: "Auto suficiente.", value: 1 },
          {
            label:
              "Requer encorajamento e supervisão da enfermagem na nutrição e hidratação oral.",
            value: 2,
          },
          {
            label:
              "Requer orientação e supervisão de enfermagem ao acompanhante para auxílio na nutrição e hidratação oral.",
            value: 3,
          },
          {
            label:
              "Requer auxílio da enfermagem na nutrição e hidratação oral e/ou assistência de enfermagem na alimentação por sonda nasogástrica ou nasoenteral ou estoma.",
            value: 4,
          },
          {
            label:
              "Requer assistência efetiva da enfermagem para manipulação de cateteres periféricos ou centrais para nutrição e hidratação.",
            value: 5,
          },
        ],
      },
      {
        key: "motilidade",
        text: "5 - Motilidade (capacidade de movimentar os segmentos corporais de forma independente, com auxílio do acompanhante ou da equipe de enfermagem ou pelo uso de artefatos)",
        options: [
          { label: "Auto suficiente.", value: 1 },
          {
            label:
              "Requer estímulo e supervisão da enfermagem para a movimentação de um ou mais segmentos corporais.",
            value: 2,
          },
          {
            label:
              "Requer orientação e supervisão de enfermagem ao acompanhante para auxílio na movimentação de um ou mais segmentos corporais.",
            value: 3,
          },
          {
            label:
              "Requer auxílio da enfermagem para a movimentação de um ou mais segmentos corporais.",
            value: 4,
          },
          {
            label:
              "Requer assistência efetiva da enfermagem para movimentação de qualquer segmento corporal devido a presença de aparelhos gessados, tração, fixador externo e outros, ou por déficit motor.",
            value: 5,
          },
        ],
      },
      {
        key: "locomoção",
        text: "6 - Locomoção (habilidade para movimentar-se dentro do ambiente físico por si só, com auxílio do acompanhante ou da equipe de enfermagem ou pelo uso de artefatos)",
        options: [
          { label: "Auto suficiente.", value: 1 },
          {
            label:
              "Requer encorajamento e supervisão da enfermagem para deambulação.",
            value: 2,
          },
          {
            label:
              "Requer orientação e supervisão de enfermagem ao acompanhante para auxílio no uso de artefatos (órteses, próteses, muletas, bengalas, cadeiras de rodas, andadores).",
            value: 3,
          },
          {
            label:
              "Requer o auxílio da enfermagem no uso de artefatos para a deambulação.",
            value: 4,
          },
          {
            label:
              "Requer assistência efetiva de enfermagem para locomoção devido `a restrição no leito.",
            value: 5,
          },
        ],
      },
      {
        key: "cuidado_corporal",
        text: "7 - Cuidado Corporal (capacidade para realizar por si mesmo ou com auxílio de outros, atividades de higiene pessoal e conforto, de vestir-se e arrumar-se)",
        options: [
          { label: "Auto suficiente.", value: 1 },
          {
            label:
              "Requer supervisão de enfermagem na realização do cuidado corporal e conforto.",
            value: 2,
          },
          {
            label:
              "Requer orientação e supervisão de enfermagem ao acompanhante para auxílio na higiene oral, higiene íntima, banho de chuveiro e medidas de conforto.",
            value: 3,
          },
          {
            label:
              "Requer auxílio da enfermagem na higiene oral, higiene íntima, banho de chuveiro e medidas de conforto.",
            value: 4,
          },
          {
            label:
              "Requer assistência efetiva da enfermagem para o cuidado corporal e medidas de conforto devido à restrição no leito.",
            value: 5,
          },
        ],
      },
      {
        key: "eliminacoes",
        text: "8 - Eliminações (habilidade em manter as diversas formas de eliminações sozinho, com auxílio do acompanhante ou da enfermagem ou por drenos e estornas)",
        options: [
          { label: "Auto suficiente.", value: 1 },
          {
            label:
              "Requer supervisão e controle pela enfermagem das eliminações.",
            value: 2,
          },
          {
            label:
              "Requer orientação e supervisão de enfermagem ao acompanhante para auxílio no uso de comadre, papagaio, troca de fraldas, absorventes e outros, e controle, pela enfermagem, das eliminações.",
            value: 3,
          },
          {
            label:
              "Requer auxílio e controle pela enfermagem no uso de comadre, papagaio, troca de fraldas, absorventes e outros.",
            value: 4,
          },
          {
            label:
              "Requer assistência efetiva de enfermagem para manipulação e controle de cateteres, drenos, dispositivo para incontinência urinária ou estomas.",
            value: 5,
          },
        ],
      },
      {
        key: "terapeutica",
        text: "9 - Terapêutica (utilização dos diversos agentes terapêuticos medicamentosos prescritos)",
        options: [
          { label: "Requer medicação VO de rotina ou ID, SC ou IM.", value: 1 },
          {
            label:
              "Requer medicação EV contínua e/ou através de sonda nasogástrica, nasoenteral ou estorna. ",
            value: 2,
          },
          {
            label:
              "Requer medicação EV intermitente com manutenção de cateter.",
            value: 3,
          },
          {
            label:
              "Requer uso de sangue e derivados ou expansores plasmáticos ou agentes citostáticos.",
            value: 4,
          },
          {
            label:
              "Requer uso de drogas vasoativas ou outras que exigem maiores cuidados na administração.",
            value: 5,
          },
        ],
      },
      {
        key: "educacao_saude",
        text: "10 - Educação à Saúde (habilidade do paciente/família em receber e aceitar orientações sobre auto- cuidado)",
        options: [
          {
            label:
              "Orientações de enfermagem ao paciente/família sobre auto cuidado com pronta compreensão e aceitação das informações recebidas.",
            value: 1,
          },
          {
            label:
              "Orientações de enfermagem ao paciente/família sobre auto cuidado com dificuldades de compreensão mas com pronta aceitação das informações recebidas.",
            value: 2,
          },
          {
            label:
              "Orientações de enfermagem ao paciente/família sobre auto cuidado com pronta compreensão mas certa resistência às informações recebidas.",
            value: 3,
          },
          {
            label:
              "Orientações de enfermagem ao paciente/família sobre auto cuidado com pronta compreensão mas elevada resistência às informações recebidas.",
            value: 4,
          },
          {
            label:
              "Orientações de enfermagem ao paciente/família sobre auto cuidado com pronta compreensão mas sem aceitação das informações recebidas.",
            value: 5,
          },
        ],
      },
      {
        key: "comportamento",
        text: "11 - Comportamento (sentimentos, pensamentos e condutas do paciente com relação à sua doença, gerados em sua interação com o processo de hospitalização, a equipe de saúde e/ou família)",
        options: [
          { label: "Calmo, tranquilo, preocupações cotidianas", value: 1 },
          {
            label:
              "Alguns sintomas de ansiedade (até 3) ou queixas e solicitações contínuas ou retraimento social.",
            value: 2,
          },
          {
            label:
              "Irritabilidade excessiva ou retraimento social aumentado ou apatia ou passividade ou queixas excessivas.",
            value: 3,
          },
          {
            label:
              "Sentimento de desesperança ou impotência psíquica ou ambivalência de sentimentos ou acentuada diminuição do interesse por atividades ou aumento da freqüência de sintomas de ansiedade (mais de 3 sintomas).",
            value: 4,
          },
          {
            label:
              "Comportamento destrutivo dirigido a si mesmo e aos outros ou recusa de cuidados de atenção à saúde ou verbalizações hostis e ameaçadoras ou completo isolamento social.",
            value: 5,
          },
        ],
      },
      {
        key: "comunicacao",
        text: "12 - Comunicação (habilidade em usar ou entender a linguagem verbal e não verbal na interação humana)",
        options: [
          {
            label: "Comunicativo, expressa ideias  com clareza e lógica.",
            value: 1,
          },
          {
            label:
              "Dificuldade em se expressar por diferenças sócio culturais; verbalização inapropriada.",
            value: 2,
          },
          {
            label: "Recusa-se a falar; choroso; comunicação não verbal.",
            value: 3,
          },
          {
            label:
              "Dificuldade em se comunicar por distúrbios de linguagem (afasia, disfasia, disartria) ou sensibilidade dolorosa ao falar ou por barreira física (traqueostomia, entubação) ou deficiência física ou mental.",
            value: 4,
          },
          { label: "Inapto para comunicar necessidades.", value: 5 },
        ],
      },
      {
        key: "integridade_cutaneo_mucosa",
        text: "13 - Integridade Cutâneo Mucosa (manutenção da pele e mucosas sem danificação ou destruição)",
        options: [
          {
            label:
              "Pele íntegra e sem alteração de cor em todas as 80s do corpo.",
            value: 1,
          },
          {
            label:
              "Presença de alteração da cor da pele (equimose, hiperemia ou outras) em uma ou mais Áreas do corpo sem solução de continuidade.",
            value: 2,
          },
          {
            label:
              "Presença de solução de continuidade em uma ou mais Áreas do corpo sem presença de exsudato purulento.",
            value: 3,
          },
          {
            label:
              "Presença de solução de continuidade em uma ou mais Áreas do corpo com presença de exsudato purulento, sem exposição de tecido muscular e/ou ósseo; ausência de Áreas de necrose..",
            value: 4,
          },
          {
            label:
              "Presença de solução de continuidade em uma ou mais Áreas do corpo com presença de exsudato purulento, exposição de tecido muscular e/ou ósseo ; presença de Áreas de necrose..",
            value: 5,
          },
        ],
      },
    ],
  },
  DINI: {
    scp: SCPType.DINI,
    title: "DINI",
    description:
      "Escala DINI para pacientes pediátricos (11 itens, opções 1..4).",
    questions: [
      {
        key: "atividade",
        text: "1 - Atividade: possibilidade de interagir e realizar atividades compatíveis com a idade",
        options: [
          {
            label:
              "Demonstração de afeto e interesse; realiza atividades compatíveis com a faixa etária.",
            value: 1,
          },
          {
            label:
              "Demonstração de afeto e interesse com limitação para realizar atividades compatíveis.",
            value: 2,
          },
          {
            label:
              "Desinteresse a estímulos por dor/tristeza/agitação; dificuldades de linguagem; déficit no desenvolvimento.",
            value: 3,
          },
          {
            label:
              "Paralisia cerebral severa ou coma vigil/inconsciente/totalmente sedado.",
            value: 4,
          },
        ],
      },
      {
        key: "intervalo_afericao",
        text: "2 - Intervalo de aferição de controles (sinais vitais, SaO2, glicemia, diálise, balanço hídrico)",
        options: [
          { label: "6/6 horas.", value: 1 },
          { label: "4/4 horas.", value: 2 },
          { label: "2/2 horas.", value: 3 },
          {
            label: "Intervalo < 2 horas ou monitorização contínua.",
            value: 4,
          },
        ],
      },
      {
        key: "oxigenacao",
        text: "3 - Oxigenação: manutenção das vias aéreas e oxigenação",
        options: [
          {
            label:
              "Respiração espontânea, sem necessidade de oxigenoterapia ou desobstrução.",
            value: 1,
          },
          {
            label:
              "Respiração espontânea, com desobstrução por instilação de soro.",
            value: 2,
          },
          {
            label:
              "Respiração espontânea, com aspiração de secreções e/ou necessidade de oxigenoterapia.",
            value: 3,
          },
          {
            label: "Ventilação mecânica (não invasiva ou invasiva).",
            value: 4,
          },
        ],
      },
      {
        key: "terapeutica_medicamentosa",
        text: "4 - Terapêutica Medicamentosa: necessidade de medicações",
        options: [
          { label: "Não necessita de medicamentos.", value: 1 },
          {
            label:
              "Medicamentos tópicos, oculares e/ou orais com paciente colaborativo.",
            value: 2,
          },
          {
            label:
              "Medicamentos parenterais/enterais/inalatórios ou via tópica com paciente não colaborativo.",
            value: 3,
          },
          {
            label:
              "Hemoderivados, quimioterápicos ou indicação absoluta de bomba de infusão.",
            value: 4,
          },
        ],
      },
      {
        key: "integridade_cutaneo_mucosa",
        text: "5 - Integridade cutâneo-mucosa: necessidade de manutenção/restauração",
        options: [
          { label: "Pele íntegra em toda a área corpórea.", value: 1 },
          {
            label:
              "Cuidados de baixa complexidade (hidratação, tratamento de dermatites simples, renovação de fixação de cateter periférico).",
            value: 2,
          },
          {
            label:
              "Cuidados de média complexidade (curativos em feridas limitadas, inserção de drenos, traqueostomia, gastrostomia, cateter venoso central).",
            value: 3,
          },
          {
            label:
              "Cuidados de alta complexidade (desbridamentos, queimaduras extensas, estomas complexos, feridas com exposição de fáscia ou músculo).",
            value: 4,
          },
        ],
      },
      {
        key: "alimentacao_hidratacao",
        text: "6 - Alimentação e hidratação",
        options: [
          {
            label: "Via oral independente ou amamentação eficaz.",
            value: 1,
          },
          {
            label: "Via oral com auxílio e paciente colaborativo.",
            value: 2,
          },
          {
            label:
              "Sondas (gástrica/enteral/gastrostomia) ou via oral com paciente não colaborativo/risco de aspiração.",
            value: 3,
          },
          { label: "Nutrição/hidratação parenteral.", value: 4 },
        ],
      },
      {
        key: "eliminacoes",
        text: "7 - Eliminações",
        options: [
          { label: "Vaso sanitário sem auxílio.", value: 1 },
          { label: "Vaso sanitário com auxílio.", value: 2 },
          {
            label: "Treino de esfíncteres ou fraldas/comadre/urinol.",
            value: 3,
          },
          { label: "Sonda vesical ou estomas.", value: 4 },
        ],
      },
      {
        key: "higiene_cuidado_corporal",
        text: "8 - Higiene e cuidado corporal",
        options: [
          { label: "Banho de aspersão sem auxílio.", value: 1 },
          { label: "Banho de aspersão com auxílio parcial.", value: 2 },
          {
            label: "Banho de imersão/aspersão em cadeira ou com auxílio total.",
            value: 3,
          },
          {
            label: "Banho no leito ou em incubadora/berço aquecido.",
            value: 4,
          },
        ],
      },
      {
        key: "mobilidade_deambulação",
        text: "9 - Mobilidade e deambulação",
        options: [
          { label: "Deambulação sem auxílio.", value: 1 },
          {
            label: "Repouso no leito e mobiliza-se sem auxílio.",
            value: 2,
          },
          {
            label:
              "Repouso no leito e mobiliza-se com auxílio ou deambula com supervisão.",
            value: 3,
          },
          {
            label:
              "Restrito no leito, totalmente dependente para mudança de decúbito.",
            value: 4,
          },
        ],
      },
      {
        key: "participacao_acompanhante",
        text: "10 - Participação do acompanhante",
        options: [
          {
            label: "Acompanhante reconhece necessidades e atende.",
            value: 1,
          },
          {
            label: "Acompanhante disponível para aprender e cuidar.",
            value: 2,
          },
          {
            label:
              "Acompanhante demonstra dificuldades/indisponibilidade ou ansiedade.",
            value: 3,
          },
          {
            label:
              "Acompanhante ausente ou indisponível/agressivo ou paciente requer cuidados técnicos de alta complexidade.",
            value: 4,
          },
        ],
      },
      {
        key: "rede_apoio_suporte_familiar",
        text: "11 - Rede de apoio e suporte familiar",
        options: [
          {
            label:
              "Presença de acompanhante envolvido na prestação e planejamento de cuidados todo o tempo.",
            value: 1,
          },
          {
            label: "Presença de acompanhante por mais de 12 horas ao dia.",
            value: 2,
          },
          {
            label: "Presença de acompanhante por menos de 12 horas ao dia.",
            value: 3,
          },
          {
            label:
              "Ausência de suporte familiar ou acompanhante com doença psiquiátrica/estresse/alienação.",
            value: 4,
          },
        ],
      },
    ],
  },
};

export default scpSchemas;
