import ExcelJS from "exceljs";

export async function xlsResumoDiario(payload: {
  data: string;
  unidade: string; // nome da unidade
  numeroLeitos: number;
  ocupacao: {
    usada: number; // total sessões ativas
  };
  taxaOcupacao: number;
  distribuicao: Record<string, number>;
  colaboradores?: Array<{
    colaboradorId: string;
    nome: string;
    total: number;
    distribuicao: Record<string, number>;
  }>;
}): Promise<Buffer> {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Resumo Diário");
  ws.addRow(["Data", payload.data]);
  ws.addRow(["Unidade", payload.unidade]);
  ws.addRow(["Leitos", payload.numeroLeitos]);
  ws.addRow(["Ocupação (fonte)", `${payload.ocupacao.usada}`]);

  // payload.taxaOcupacao is decimal 0..1 — show as percentage with 2 decimals
  ws.addRow(["Taxa Ocupação", (payload.taxaOcupacao * 100).toFixed(2) + "%"]);
  ws.addRow([]);
  ws.addRow(["Distribuição"]);
  ws.addRow(["Classe", "Quantidade"]);
  for (const [k, v] of Object.entries(payload.distribuicao || {}))
    ws.addRow([k, v]);

  // Colaboradores
  if (payload.colaboradores && payload.colaboradores.length) {
    const wsCol = wb.addWorksheet("Colaboradores");
    const classes = [
      "MINIMOS",
      "INTERMEDIARIOS",
      "ALTA_DEPENDENCIA",
      "SEMI_INTENSIVOS",
      "INTENSIVOS",
    ];
    wsCol.addRow(["Nome", "Total", ...classes]);
    for (const c of payload.colaboradores) {
      wsCol.addRow([
        c.nome,
        c.total,
        ...classes.map((k) => c.distribuicao[k] || 0),
      ]);
    }
  }
  const uint8array = await wb.xlsx.writeBuffer();
  return Buffer.from(uint8array);
}

export async function xlsMensal(payload: {
  ano: number;
  mes: number;
  numeroLeitos: number;
  ocupacaoMensal: {
    avaliacao: Array<{ data: string; ocupados: number }>;
    mediaOcupadosDia: number;
    taxaOcupacaoMedia: number;
  };
  distribuicaoMensal: Record<string, number>;
  colaboradores?: Array<{
    colaboradorId: string;
    nome: string;
    total: number;
    distribuicao: Record<string, number>;
  }>;
}): Promise<Buffer> {
  const wb = new ExcelJS.Workbook();

  const ws1 = wb.addWorksheet("Ocupação");
  ws1.addRow(["Data", "Ocupados"]);
  for (const it of payload.ocupacaoMensal.avaliacao) {
    ws1.addRow([it.data, it.ocupados]);
  }
  ws1.addRow([]);
  ws1.addRow(["#Leitos", payload.numeroLeitos]);
  ws1.addRow(["Média Ocupados/Dia", payload.ocupacaoMensal.mediaOcupadosDia]);
  // taxaOcupacaoMedia is decimal 0..1 — format as percentage with 2 decimals
  ws1.addRow([
    "Taxa Média Ocupação",
    (payload.ocupacaoMensal.taxaOcupacaoMedia * 100).toFixed(2) + "%",
  ]);

  const ws2 = wb.addWorksheet("Distribuição Mensal");
  ws2.addRow(["Classe", "Quantidade"]);
  for (const [k, v] of Object.entries(payload.distribuicaoMensal || {}))
    ws2.addRow([k, v]);

  // Colaboradores no mês
  if (payload.colaboradores && payload.colaboradores.length) {
    const ws3 = wb.addWorksheet("Colaboradores");
    const classes = [
      "MINIMOS",
      "INTERMEDIARIOS",
      "ALTA_DEPENDENCIA",
      "SEMI_INTENSIVOS",
      "INTENSIVOS",
    ];
    ws3.addRow(["Nome", "Total", ...classes]);
    for (const c of payload.colaboradores) {
      ws3.addRow([
        c.nome,
        c.total,
        ...classes.map((k) => c.distribuicao[k] || 0),
      ]);
    }
  }

  const uint8array = await wb.xlsx.writeBuffer();
  return Buffer.from(uint8array);
}

export async function xlsDimensionamento(payload: {
  entrada: any;
  resultado: {
    the: number;
    istAplicado: number;
    pctEnf: number;
    pctTec: number;
    kmEnf: number;
    kmTec: number;
    qpEnf: { decimal: number; arredondado: number };
    qpTec: { decimal: number; arredondado: number };
    horasPorClasseUsadas: Record<string, number>;
  };
}): Promise<Buffer> {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Dimensionamento");
  const r = payload.resultado;

  ws.addRow(["THE", r.the]);
  ws.addRow(["IST aplicado", r.istAplicado]);
  ws.addRow(["% Enf", r.pctEnf]);
  ws.addRow(["% Tec", r.pctTec]);
  ws.addRow(["KM Enf", r.kmEnf]);
  ws.addRow(["KM Tec", r.kmTec]);
  ws.addRow(["QP Enf (dec)", r.qpEnf.decimal]);
  ws.addRow(["QP Enf (arr)", r.qpEnf.arredondado]);
  ws.addRow(["QP Tec (dec)", r.qpTec.decimal]);
  ws.addRow(["QP Tec (arr)", r.qpTec.arredondado]);

  ws.addRow([]);
  ws.addRow(["Horas por Classe"]);
  ws.addRow(["Classe", "Horas"]);
  for (const [k, v] of Object.entries(r.horasPorClasseUsadas))
    ws.addRow([k, v]);

  const uint8array = await wb.xlsx.writeBuffer();
  return Buffer.from(uint8array);
}

export async function xlsGradeMensal(payload: {
  ano: number;
  mes: number;
  dias: Array<{
    data: string;
    turnos: Array<{
      label: string;
      ENF: Array<{ nome: string }>;
      TEC: Array<{ nome: string }>;
    }>;
  }>;
}): Promise<Buffer> {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Grade Mensal");
  ws.addRow(["Data", "Turno", "ENF", "TEC"]);
  for (const dia of payload.dias) {
    for (const t of dia.turnos) {
      ws.addRow([
        dia.data,
        t.label,
        t.ENF.map((p) => p.nome).join(" | "),
        t.TEC.map((p) => p.nome).join(" | "),
      ]);
    }
  }
  const uint8array = await wb.xlsx.writeBuffer();
  return Buffer.from(uint8array);
}
