import PDFDocument from "pdfkit";
import { readFileSync } from "fs";
import path from "path";

// --- Carregamento da Logo ---
let logo: Buffer | null = null;
try {
  const logoPath = path.join(
    process.cwd(),
    "src",
    "utils",
    "exporters",
    "logo.png"
  );
  logo = readFileSync(logoPath);
} catch (error) {
  if (error instanceof Error) {
    console.error(
      "Erro ao ler o arquivo da logo. Verifique o caminho:",
      error.message
    );
  } else {
    console.error(
      "Ocorreu um erro desconhecido ao ler o arquivo da logo:",
      error
    );
  }
}

// --- Funções Auxiliares de Formatação ---
const FONT_NORMAL = "Helvetica";
const FONT_BOLD = "Helvetica-Bold";
const BRAND_COLOR = "#00a99d"; // Verde da logo

// Mapa para traduzir as classificações para texto legível
const classificationMap: Record<string, string> = {
  MINIMOS: "Cuidados Mínimos",
  INTERMEDIARIOS: "Cuidados Intermediários",
  ALTA_DEPENDENCIA: "Alta Dependência",
  SEMI_INTENSIVOS: "Cuidado Semi-Intensivo",
  INTENSIVOS: "Cuidado Intensivo",
};

/**
 * Função genérica para criar tabelas com grade completa, cabeçalho e quebra de página.
 */
function generateTable(
  doc: PDFKit.PDFDocument,
  headers: {
    text: string;
    width: number;
    align?: "left" | "center" | "right";
  }[],
  rows: string[][]
) {
  const rowHeight = 20;
  const headerHeight = 18;
  const columnWidths = headers.map((h) => h.width);
  const startX = doc.page.margins.left;

  const drawHeader = () => {
    const startY = doc.y;
    doc
      .rect(
        startX,
        startY,
        columnWidths.reduce((a, b) => a + b),
        headerHeight
      )
      .fill(BRAND_COLOR);
    doc.fontSize(9).fillColor("#FFFFFF").font(FONT_BOLD);
    let currentX = startX;
    headers.forEach((header) => {
      doc.text(header.text, currentX + 5, startY + 6, {
        width: header.width - 10,
        align: header.align || "left",
      });
      currentX += header.width;
    });
    doc.y = startY + headerHeight;
  };

  const drawRow = (rowData: string[], isEven: boolean) => {
    let startY = doc.y;
    if (startY + rowHeight > doc.page.height - doc.page.margins.bottom) {
      doc.addPage();
      drawHeader();
      startY = doc.y;
    }

    const tableWidth = columnWidths.reduce((a, b) => a + b);
    doc
      .rect(startX, startY, tableWidth, rowHeight)
      .fill(isEven ? "#f7f7f7" : "#ffffff");

    doc.fontSize(8).fillColor("#333333").font(FONT_NORMAL);
    let currentX = startX;
    rowData.forEach((cell, i) => {
      doc.text(cell, currentX + 5, startY + 7, {
        width: headers[i].width - 10,
        align: headers[i].align || "left",
      });
      currentX += headers[i].width;
    });

    doc
      .moveTo(startX, startY + rowHeight)
      .lineTo(startX + tableWidth, startY + rowHeight)
      .strokeColor("#d9d9d9")
      .lineWidth(0.5)
      .stroke();
    let accumulatedWidth = startX;
    doc
      .moveTo(accumulatedWidth, startY)
      .lineTo(accumulatedWidth, startY + rowHeight)
      .strokeColor("#d9d9d9")
      .lineWidth(0.5)
      .stroke();
    columnWidths.forEach((width) => {
      accumulatedWidth += width;
      doc
        .moveTo(accumulatedWidth, startY)
        .lineTo(accumulatedWidth, startY + rowHeight)
        .strokeColor("#d9d9d9")
        .lineWidth(0.5)
        .stroke();
    });

    doc.y = startY + rowHeight;
  };

  drawHeader();
  rows.forEach((row, i) => drawRow(row, i % 2 !== 0));
}

/**
 * Função principal que cria o documento, com cabeçalho e rodapé.
 */
function bufferFromDoc(
  title: string,
  make: (doc: PDFKit.PDFDocument) => void
): Promise<Buffer> {
  return new Promise((resolve) => {
    const doc = new PDFDocument({ margin: 40, size: "A4", bufferPages: true });
    const chunks: Buffer[] = [];

    doc.on("data", (c) => chunks.push(c as Buffer));
    doc.on("end", () => resolve(Buffer.concat(chunks)));

    const drawHeader = () => {
      if (logo) {
        doc.image(logo, 40, 25, { width: 90 });
      }
      doc
        .fontSize(14)
        .font(FONT_BOLD)
        .fillColor("#333333")
        .text(title, { align: "center" });
      doc
        .fontSize(8)
        .font(FONT_NORMAL)
        .fillColor("#666666")
        .text(
          `Emitido em: ${new Date().toLocaleDateString("pt-BR")}`,
          doc.page.width - 40 - 100,
          40,
          {
            align: "right",
            width: 100,
          }
        );
      doc
        .moveTo(40, 75)
        .lineTo(doc.page.width - 40, 75)
        .strokeColor("#cccccc")
        .stroke();
    };

    doc.on("pageAdded", () => {
      drawHeader();
      doc.y = 90;
    });

    drawHeader();
    doc.y = 90;

    make(doc);

    const range = doc.bufferedPageRange();
    for (let i = range.start; i < range.start + range.count; i++) {
      doc.switchToPage(i);
      doc
        .fontSize(8)
        .fillColor("#666666")
        .text(`Página ${i + 1} de ${range.count}`, 0, doc.page.height - 30, {
          align: "center",
        });
    }

    doc.end();
  });
}

// --- Funções de Geração de PDF ---

export async function pdfResumoDiario(payload: {
  data: string;
  unidade: string;
  numeroLeitos: number;
  ocupacao: { usada: number };
  taxaOcupacao: number;
  distribuicao: Record<string, number>;
  colaboradores?: Array<{
    nome: string;
    total: number;
    distribuicao: Record<string, number>;
  }>;
  avaliacoes?: Array<{
    leito?: { numero?: string; status?: string };
    created_at?: string | Date;
    autor?: { nome?: string };
    classificacao?: string;
  }>;
}): Promise<Buffer> {
  return bufferFromDoc("Resumo Diário da Unidade", (doc) => {
    doc.y += 10;
    doc
      .rect(40, doc.y, doc.page.width - 80, 80)
      .strokeColor("#eeeeee")
      .stroke();
    const summaryY = doc.y;
    doc
      .fontSize(10)
      .font(FONT_BOLD)
      .text("Data:", 50, summaryY + 10)
      .text("Unidade:", 50, summaryY + 25)
      .text("Total de Leitos:", 50, summaryY + 40)
      .text("Ocupação Atual:", 50, summaryY + 55);
    doc
      .font(FONT_NORMAL)
      .text(
        new Date(payload.data).toLocaleDateString("pt-BR", { timeZone: "UTC" }),
        150,
        summaryY + 10
      )
      .text(payload.unidade, 150, summaryY + 25)
      .text(`${payload.numeroLeitos}`, 150, summaryY + 40)
      .text(`${payload.ocupacao.usada}`, 150, summaryY + 55);
    doc.font(FONT_BOLD).text("Taxa de Ocupação:", 300, summaryY + 10);
    doc
      .font(FONT_NORMAL)
      // payload.taxaOcupacao is decimal 0..1 — display as percentage with 2 decimals
      .text(`${(payload.taxaOcupacao * 100).toFixed(2)}%`, 420, summaryY + 10);
    doc.y = summaryY + 95;

    if (payload.avaliacoes && payload.avaliacoes.length) {
      doc
        .fontSize(12)
        .font(FONT_BOLD)
        .text("Avaliações Realizadas", { align: "center" })
        .moveDown(0.5);
      const headers = [
        { text: "Leito", width: 80, align: "center" as const },
        { text: "Horário", width: 100, align: "center" as const },
        { text: "Autor", width: 200 },
        { text: "Classificação", width: 135 },
      ];
      const rows = payload.avaliacoes.map((a) => [
        a.leito?.numero || "-",
        a.created_at ? new Date(a.created_at).toLocaleTimeString("pt-BR") : "-",
        a.autor?.nome || "-",
        a.classificacao || "-",
      ]);
      generateTable(doc, headers, rows);
    }
    doc.moveDown(2);

    // Tabela de Distribuição das Classificações
    doc
      .fontSize(12)
      .font(FONT_BOLD)
      .text("Distribuição das Classificações", doc.page.margins.left, doc.y, {
        align: "center",
        width: doc.page.width - doc.page.margins.left - doc.page.margins.right,
      });
    doc.moveDown(1.5);

    const distribuicaoHeaders = [
      { text: "Classificação", width: 300, align: "left" as const },
      { text: "Quantidade", width: 215, align: "center" as const },
    ];

    const classificacoes = [
      { key: "MINIMOS", label: "Cuidados Mínimos" },
      { key: "INTERMEDIARIOS", label: "Cuidados Intermediários" },
      { key: "ALTA_DEPENDENCIA", label: "Alta Dependência" },
      { key: "SEMI_INTENSIVOS", label: "Cuidados Semi-Intensivos" },
      { key: "INTENSIVOS", label: "Cuidados Intensivos" },
    ];

    const distribuicaoRows = classificacoes.map((cls) => [
      cls.label,
      `${payload.distribuicao[cls.key] || 0}`,
    ]);

    generateTable(doc, distribuicaoHeaders, distribuicaoRows);

    const legendY = doc.page.height - 70;
    if (doc.y < legendY - 20) {
      doc.y = legendY - 20;
    }
    doc
      .moveTo(40, doc.y)
      .lineTo(doc.page.width - 40, doc.y)
      .strokeColor("#cccccc")
      .stroke()
      .moveDown(0.5);
    doc.fontSize(9).font(FONT_BOLD).text("Legenda:", 40, doc.y);
    doc
      .fontSize(8)
      .font(FONT_NORMAL)
      .text(
        "MIN: Cuidados Mínimos  •  INT: Cuidados Intermediários  •  AD: Alta Dependência  •  SI: Cuidado Semi-Intensivo  •  IN: Cuidado Intensivo",
        { align: "left", lineGap: 2 }
      );
  });
}

export async function pdfListagemLeitos(payload: {
  data: string;
  unidade: string;
  leitos: Array<{
    numero: string;
    status: string;
    classificacao?: string;
  }>;
}): Promise<Buffer> {
  const title = `Listagem de Leitos - ${payload.unidade}`;
  return bufferFromDoc(title, (doc) => {
    doc.y += 10;
    doc
      .fontSize(12)
      .font(FONT_BOLD)
      .text("Situação dos Leitos", { align: "center" });
    doc
      .fontSize(10)
      .font(FONT_NORMAL)
      .text(
        `Relatório do dia: ${new Date(payload.data).toLocaleDateString(
          "pt-BR",
          { timeZone: "UTC" }
        )}`,
        { align: "center" }
      );
    doc.moveDown(1.5);

    const headers = [
      { text: "Leito", width: 120, align: "left" as const },
      { text: "Status", width: 150, align: "left" as const },
      { text: "Classificação do Paciente", width: 245, align: "left" as const },
    ];
    const rows = payload.leitos.map((leito) => {
      const classificacaoTexto = leito.classificacao
        ? classificationMap[leito.classificacao.toUpperCase()] ||
          leito.classificacao
        : "-";
      return [leito.numero, leito.status, classificacaoTexto];
    });
    generateTable(doc, headers, rows);
  });
}

export async function pdfMensal(payload: {
  ano: number;
  mes: number;
  unidade: string;
  numeroLeitos: number;
  ocupacaoMensal: {
    mediaOcupadosDia: number;
    taxaOcupacaoMedia: number;
    avaliacao: Array<{ data: string; ocupados: number }>;
  };
  distribuicaoMensal: Record<string, number>;
  colaboradores?: Array<{
    nome: string;
    total: number;
    distribuicao: Record<string, number>;
  }>;
}): Promise<Buffer> {
  const title = `Relatório Mensal - ${payload.unidade}`;
  return bufferFromDoc(title, (doc) => {
    doc.y += 10;
    doc
      .rect(40, doc.y, doc.page.width - 80, 80)
      .strokeColor("#eeeeee")
      .stroke();
    const summaryY = doc.y;
    doc
      .fontSize(10)
      .font(FONT_BOLD)
      .text("Período:", 50, summaryY + 10)
      .text("Leitos na Unidade:", 50, summaryY + 25)
      .text("Média de Ocupação:", 50, summaryY + 40);
    doc
      .font(FONT_NORMAL)
      .text(
        `${String(payload.mes).padStart(2, "0")}/${payload.ano}`,
        160,
        summaryY + 10
      )
      .text(`${payload.numeroLeitos}`, 160, summaryY + 25)
      .text(
        `${payload.ocupacaoMensal.mediaOcupadosDia.toFixed(2)} / dia`,
        160,
        summaryY + 40
      );
    doc
      .font(FONT_BOLD)
      .text("Taxa Média Ocupação:", 300, summaryY + 10)
      .text("Distribuição Geral:", 300, summaryY + 25);
    const distribuicaoText = Object.entries(payload.distribuicaoMensal || {})
      .map(([k, v]) => `${k.slice(0, 3)}: ${v}`)
      .join(" / ");
    doc
      .font(FONT_NORMAL)
      // taxaOcupacaoMedia stored as decimal 0..1 — show as percentage with 2 decimals
      .text(
        `${(payload.ocupacaoMensal.taxaOcupacaoMedia * 100).toFixed(2)}%`,
        430,
        summaryY + 10
      )
      .text(distribuicaoText, 430, summaryY + 25, { width: 140 });
    doc.y = summaryY + 95;

    // AJUSTE: Título "Ocupação Diária da Unidade" centralizado e em uma linha
    doc
      .fontSize(12)
      .font(FONT_BOLD)
      .text("Ocupação Diária da Unidade", doc.page.margins.left, doc.y, {
        align: "center",
        width: doc.page.width - doc.page.margins.left - doc.page.margins.right,
      });
    doc.moveDown(1.5); // Espaço depois do título
    const ocupacaoHeaders = [
      { text: "Data", width: 257 },
      { text: "Leitos Ocupados", width: 258, align: "center" as const },
    ];
    const ocupacaoRows = payload.ocupacaoMensal.avaliacao.map((item) => [
      new Date(item.data).toLocaleDateString("pt-BR", { timeZone: "UTC" }),
      `${item.ocupados}`,
    ]);
    generateTable(doc, ocupacaoHeaders, ocupacaoRows);
    doc.moveDown(2);

    if (payload.colaboradores && payload.colaboradores.length) {
      doc
        .fontSize(12)
        .font(FONT_BOLD)
        .text(
          "Desempenho Mensal dos Colaboradores",
          doc.page.margins.left,
          doc.y,
          {
            align: "center",
            width:
              doc.page.width - doc.page.margins.left - doc.page.margins.right,
          }
        );
      doc.moveDown(1.5);
      const headers = [
        { text: "Nome do Colaborador", width: 235 },
        { text: "Total", width: 40, align: "center" as const },
        { text: "MIN", width: 40, align: "center" as const },
        { text: "INT", width: 40, align: "center" as const },
        { text: "AD", width: 40, align: "center" as const },
        { text: "SI", width: 40, align: "center" as const },
        { text: "IN", width: 40, align: "center" as const },
      ];
      const rows = payload.colaboradores.map((c) => [
        c.nome,
        `${c.total}`,
        `${c.distribuicao["MINIMOS"] || 0}`,
        `${c.distribuicao["INTERMEDIARIOS"] || 0}`,
        `${c.distribuicao["ALTA_DEPENDENCIA"] || 0}`,
        `${c.distribuicao["SEMI_INTENSIVOS"] || 0}`,
        `${c.distribuicao["INTENSIVOS"] || 0}`,
      ]);
      generateTable(doc, headers, rows);
    }

    const legendY = doc.page.height - 70;
    if (doc.y < legendY - 20) {
      doc.y = legendY - 20;
    }
    doc
      .moveTo(40, doc.y)
      .lineTo(doc.page.width - 40, doc.y)
      .strokeColor("#cccccc")
      .stroke()
      .moveDown(0.5);
    doc.fontSize(9).font(FONT_BOLD).text("Legenda:", 40, doc.y);
    doc
      .fontSize(8)
      .font(FONT_NORMAL)
      .text(
        "MIN: Cuidados Mínimos  •  INT: Cuidados Intermediários  •  AD: Alta Dependência  •  SI: Cuidado Semi-Intensivo  •  IN: Cuidado Intensivo",
        { align: "left", lineGap: 2 }
      );
  });
}

export async function pdfConsolidadoMensal(payload: {
  unidade: string;
  dataInicial: string;
  dataFinal: string;
  historicoMensal: Array<{
    mesAno: string;
    cuidadosMinimos: number;
    cuidadosIntermediarios: number;
    cuidadosAltaDependencia: number;
    cuidadosSemiIntensivos: number;
    cuidadosIntensivos: number;
    somaLeitos: number;
    leitosOperacionais: number;
    percentualOcupacao: number;
  }>;
}): Promise<Buffer> {
  const title = `Relatório Consolidado Mensal - ${payload.unidade}`;
  return bufferFromDoc(title, (doc) => {
    doc.y += 10;

    // Cabeçalho do relatório
    doc
      .rect(40, doc.y, doc.page.width - 80, 60)
      .strokeColor("#eeeeee")
      .stroke();
    const summaryY = doc.y;
    doc
      .fontSize(10)
      .font(FONT_BOLD)
      .text("Período:", 50, summaryY + 10)
      .text("Unidade:", 50, summaryY + 25)
      .text("Total de Meses:", 50, summaryY + 40);

    doc
      .font(FONT_NORMAL)
      .text(
        `${payload.dataInicial} até ${payload.dataFinal}`,
        160,
        summaryY + 10
      )
      .text(payload.unidade, 160, summaryY + 25)
      .text(`${payload.historicoMensal.length}`, 160, summaryY + 40);

    doc.y = summaryY + 75;

    // Título da tabela
    doc
      .fontSize(12)
      .font(FONT_BOLD)
      .text("Histórico Mensal de Ocupação", doc.page.margins.left, doc.y, {
        align: "center",
        width: doc.page.width - doc.page.margins.left - doc.page.margins.right,
      });
    doc.moveDown(1.5);

    // Cabeçalhos da tabela
    const headers = [
      { text: "Mês/Ano", width: 60, align: "center" as const },
      { text: "Min", width: 35, align: "center" as const },
      { text: "Int", width: 35, align: "center" as const },
      { text: "AD", width: 35, align: "center" as const },
      { text: "SI", width: 35, align: "center" as const },
      { text: "In", width: 35, align: "center" as const },
      { text: "Soma Leitos", width: 70, align: "center" as const },
      { text: "Operacionais", width: 75, align: "center" as const },
      { text: "% Ocupação", width: 65, align: "center" as const },
    ];

    // Linhas de dados
    const rows = payload.historicoMensal.map((item) => [
      item.mesAno,
      item.cuidadosMinimos.toString(),
      item.cuidadosIntermediarios.toString(),
      item.cuidadosAltaDependencia.toString(),
      item.cuidadosSemiIntensivos.toString(),
      item.cuidadosIntensivos.toString(),
      item.somaLeitos.toString(),
      item.leitosOperacionais.toString(),
      `${item.percentualOcupacao}%`,
    ]);

    generateTable(doc, headers, rows);

    // Calcular médias
    if (payload.historicoMensal.length > 0) {
      doc.moveDown(1);

      const totais = payload.historicoMensal.reduce(
        (acc, item) => ({
          cuidadosMinimos: acc.cuidadosMinimos + item.cuidadosMinimos,
          cuidadosIntermediarios:
            acc.cuidadosIntermediarios + item.cuidadosIntermediarios,
          cuidadosAltaDependencia:
            acc.cuidadosAltaDependencia + item.cuidadosAltaDependencia,
          cuidadosSemiIntensivos:
            acc.cuidadosSemiIntensivos + item.cuidadosSemiIntensivos,
          cuidadosIntensivos: acc.cuidadosIntensivos + item.cuidadosIntensivos,
          somaLeitos: acc.somaLeitos + item.somaLeitos,
          leitosOperacionais: acc.leitosOperacionais + item.leitosOperacionais,
          percentualOcupacao: acc.percentualOcupacao + item.percentualOcupacao,
        }),
        {
          cuidadosMinimos: 0,
          cuidadosIntermediarios: 0,
          cuidadosAltaDependencia: 0,
          cuidadosSemiIntensivos: 0,
          cuidadosIntensivos: 0,
          somaLeitos: 0,
          leitosOperacionais: 0,
          percentualOcupacao: 0,
        }
      );

      const medias = {
        cuidadosMinimos: Math.round(
          totais.cuidadosMinimos / payload.historicoMensal.length
        ),
        cuidadosIntermediarios: Math.round(
          totais.cuidadosIntermediarios / payload.historicoMensal.length
        ),
        cuidadosAltaDependencia: Math.round(
          totais.cuidadosAltaDependencia / payload.historicoMensal.length
        ),
        cuidadosSemiIntensivos: Math.round(
          totais.cuidadosSemiIntensivos / payload.historicoMensal.length
        ),
        cuidadosIntensivos: Math.round(
          totais.cuidadosIntensivos / payload.historicoMensal.length
        ),
        somaLeitos: Math.round(
          totais.somaLeitos / payload.historicoMensal.length
        ),
        leitosOperacionais: Math.round(
          totais.leitosOperacionais / payload.historicoMensal.length
        ),
        percentualOcupacao: Math.round(
          totais.percentualOcupacao / payload.historicoMensal.length
        ),
      };

      // Linha de média destacada
      const startY = doc.y;
      const tableWidth = headers.reduce((acc, h) => acc + h.width, 0);
      doc
        .rect(40, startY, tableWidth, 20)
        .fill("#e8f4f8")
        .strokeColor("#00a99d")
        .stroke();

      doc.fontSize(8).fillColor("#333333").font(FONT_BOLD);
      const mediaRow = [
        "MÉDIA",
        medias.cuidadosMinimos.toString(),
        medias.cuidadosIntermediarios.toString(),
        medias.cuidadosAltaDependencia.toString(),
        medias.cuidadosSemiIntensivos.toString(),
        medias.cuidadosIntensivos.toString(),
        medias.somaLeitos.toString(),
        medias.leitosOperacionais.toString(),
        `${medias.percentualOcupacao}%`,
      ];

      let currentX = 40;
      mediaRow.forEach((cell, i) => {
        doc.text(cell, currentX + 5, startY + 7, {
          width: headers[i].width - 10,
          align: headers[i].align || "left",
        });
        currentX += headers[i].width;
      });
      doc.y = startY + 25;
    }

    // Legenda
    const legendY = doc.page.height - 70;
    if (doc.y < legendY - 20) {
      doc.y = legendY - 20;
    }
    doc
      .moveTo(40, doc.y)
      .lineTo(doc.page.width - 40, doc.y)
      .strokeColor("#cccccc")
      .stroke()
      .moveDown(0.5);
    doc.fontSize(9).font(FONT_BOLD).text("Legenda:", 40, doc.y);
    doc
      .fontSize(8)
      .font(FONT_NORMAL)
      .text(
        "Min: Cuidados Mínimos  •  Int: Intermediários  •  AD: Alta Dependência  •  SI: Semi-Intensivo  •  In: Intensivo",
        { align: "left", lineGap: 2 }
      );
  });
}

export async function pdfDimensionamento(payload: {
  unidade: {
    nome: string;
    scpMetodoKey?: string;
    numeroLeitos: number;
  };
  dimensionamento: {
    enfermeiroCargoHorario: number;
    enfermeiroPercentualEquipe: number;
    tecnicoEnfermagemCargoHorario: number;
    tecnicoEnfermagemPercentualEquipe: number;
    indiceTecnico: number;
    quantidadeLeitos: number;
    taxaOcupacao: number;
    diasSemana: number;
    idadeEquipeRestricoes: string;
    pcm: number;
    pci: number;
    pcad: number;
    pcsi: number;
    pcit: number;
    constanteMarinhoEnfermeiro: number;
    constanteMarinhoTecnico: number;
    totalHorasEnfermagem: number;
    quadroPessoalTotal: number;
    quadroPessoalEnfermeiro: number;
    quadroPessoalTecnico: number;
  };
}): Promise<Buffer> {
  const title = `Dimensionamento de Enfermagem - ${payload.unidade.nome}`;
  return bufferFromDoc(title, (doc) => {
    doc.y += 10;

    // Informações da Unidade
    doc
      .rect(40, doc.y, doc.page.width - 80, 80)
      .strokeColor("#eeeeee")
      .stroke();
    const summaryY = doc.y;
    doc
      .fontSize(10)
      .font(FONT_BOLD)
      .text("Unidade:", 50, summaryY + 10)
      .text("Método SCP:", 50, summaryY + 25)
      .text("Total de Leitos:", 50, summaryY + 40)
      .text("Data/Hora:", 50, summaryY + 55);

    doc
      .font(FONT_NORMAL)
      .text(payload.unidade.nome, 160, summaryY + 10)
      .text(payload.unidade.scpMetodoKey || "Não informado", 160, summaryY + 25)
      .text(`${payload.unidade.numeroLeitos}`, 160, summaryY + 40)
      .text(new Date().toLocaleString("pt-BR"), 160, summaryY + 55);

    doc.y = summaryY + 95;

    // Configuração da Equipe
    doc
      .fontSize(12)
      .font(FONT_BOLD)
      .text("Configuração da Equipe", { align: "center" });
    doc.moveDown(1);

    const equipeHeaders = [
      { text: "Categoria", width: 200, align: "left" as const },
      { text: "Carga Horária", width: 100, align: "center" as const },
      { text: "% da Equipe", width: 100, align: "center" as const },
      { text: "Observações", width: 115, align: "center" as const },
    ];

    const equipeRows = [
      [
        "Enfermeiro",
        `${payload.dimensionamento.enfermeiroCargoHorario}h`,
        `${(payload.dimensionamento.enfermeiroPercentualEquipe * 100).toFixed(
          1
        )}%`,
        "-",
      ],
      [
        "Técnico de Enfermagem",
        `${payload.dimensionamento.tecnicoEnfermagemCargoHorario}h`,
        `${(
          payload.dimensionamento.tecnicoEnfermagemPercentualEquipe * 100
        ).toFixed(1)}%`,
        "-",
      ],
    ];

    generateTable(doc, equipeHeaders, equipeRows);
    doc.moveDown(1.5);

    // Parâmetros
    doc
      .fontSize(12)
      .font(FONT_BOLD)
      .text("Parâmetros de Dimensionamento", { align: "center" });
    doc.moveDown(1);

    const parametrosHeaders = [
      { text: "Parâmetro", width: 250, align: "left" as const },
      { text: "Valor", width: 265, align: "center" as const },
    ];

    const parametrosRows = [
      [
        "Índice de Segurança Técnica (%)",
        `${payload.dimensionamento.indiceTecnico}%`,
      ],
      ["Quantidade de Leitos", `${payload.dimensionamento.quantidadeLeitos}`],
      // dimensionamento.taxaOcupacao is decimal 0..1 — display as percentage with 2 decimals
      [
        "Taxa de Ocupação (%)",
        `${(payload.dimensionamento.taxaOcupacao * 100).toFixed(2)}%`,
      ],
      ["Dias da Semana", `${payload.dimensionamento.diasSemana}`],
      [
        "Idade da Equipe/Restrições",
        payload.dimensionamento.idadeEquipeRestricoes === "sim" ? "Sim" : "Não",
      ],
    ];

    generateTable(doc, parametrosHeaders, parametrosRows);
    doc.moveDown(1.5);

    // Distribuição de Pacientes
    doc
      .fontSize(12)
      .font(FONT_BOLD)
      .text("Distribuição de Pacientes", { align: "center" });
    doc.moveDown(1);

    const pacientesHeaders = [
      { text: "Tipo de Cuidado", width: 200, align: "left" as const },
      { text: "Quantidade", width: 80, align: "center" as const },
      { text: "Horas/Paciente", width: 90, align: "center" as const },
      { text: "Total Horas", width: 145, align: "center" as const },
    ];

    const pacientesRows = [
      [
        "Cuidados Mínimos (PCM)",
        `${payload.dimensionamento.pcm}`,
        "4h",
        `${payload.dimensionamento.pcm * 4}h`,
      ],
      [
        "Cuidados Intermediários (PCI)",
        `${payload.dimensionamento.pci}`,
        "6h",
        `${payload.dimensionamento.pci * 6}h`,
      ],
      [
        "Alta Dependência (PCAD)",
        `${payload.dimensionamento.pcad}`,
        "10h",
        `${payload.dimensionamento.pcad * 10}h`,
      ],
      [
        "Semi-Intensivos (PCSI)",
        `${payload.dimensionamento.pcsi}`,
        "10h",
        `${payload.dimensionamento.pcsi * 10}h`,
      ],
      [
        "Intensivos (PCIT)",
        `${payload.dimensionamento.pcit}`,
        "18h",
        `${payload.dimensionamento.pcit * 18}h`,
      ],
    ];

    generateTable(doc, pacientesHeaders, pacientesRows);
    doc.moveDown(1.5);

    // Resultados Calculados
    doc
      .fontSize(12)
      .font(FONT_BOLD)
      .text("Resultados do Dimensionamento", { align: "center" });
    doc.moveDown(1);

    const resultadosHeaders = [
      { text: "Indicador", width: 300, align: "left" as const },
      { text: "Valor", width: 215, align: "center" as const },
    ];

    const resultadosRows = [
      [
        "Total de Horas de Enfermagem (THE)",
        `${payload.dimensionamento.totalHorasEnfermagem.toFixed(2)}h`,
      ],
      [
        "Quadro de Pessoal Total (QP)",
        `${payload.dimensionamento.quadroPessoalTotal.toFixed(
          2
        )} profissionais`,
      ],
      [
        "Quadro de Pessoal - Enfermeiros",
        `${payload.dimensionamento.quadroPessoalEnfermeiro.toFixed(
          2
        )} enfermeiros`,
      ],
      [
        "Quadro de Pessoal - Técnicos",
        `${payload.dimensionamento.quadroPessoalTecnico.toFixed(2)} técnicos`,
      ],
    ];

    generateTable(doc, resultadosHeaders, resultadosRows);
    doc.moveDown(2);

    // Fórmula
    doc
      .fontSize(10)
      .font(FONT_BOLD)
      .text("Fórmula Utilizada:", { align: "left" });
    doc
      .fontSize(9)
      .font(FONT_NORMAL)
      .text(
        "QP = [(PCM × 4) + (PCI × 6) + (PCAD × 10) + (PCSI × 10) + (PCIT × 18)] × DS × IST / CHS",
        { align: "left" }
      )
      .moveDown(0.5)
      .text("Onde:", { align: "left" })
      .text("• QP = Quadro de Pessoal", { align: "left" })
      .text("• DS = Dias da Semana", { align: "left" })
      .text("• IST = Índice de Segurança Técnica", { align: "left" })
      .text("• CHS = Carga Horária Semanal", { align: "left" });
  });
}
