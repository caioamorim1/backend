import {
  Entity,
  PrimaryGeneratedColumn,
  ManyToOne,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from "typeorm";
import { Leito, StatusLeito } from "./Leito";
import { ClassificacaoCuidado } from "./AvaliacaoSCP";

@Entity("historicos_ocupacao")
@Index(["leito", "inicio"]) // já existente
@Index(["leito", "fim"]) // novo índice auxiliar
export class HistoricoOcupacao {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  // Campo leito volta a ser obrigatório e bloqueia deleção caso haja histórico (RESTRICT)
  @ManyToOne(() => Leito, { nullable: false, onDelete: "RESTRICT" })
  leito!: Leito;

  // Snapshots para preservar contexto no tempo
  @Column({ type: "uuid", nullable: true })
  unidadeId!: string | null; // unidade do leito no momento do registro

  @Column({ type: "uuid", nullable: true })
  hospitalId!: string | null; // hospital da unidade no momento do registro

  @Column({ type: "varchar", length: 50, nullable: true })
  leitoNumero!: string | null; // número/identificação do leito no momento

  // snapshot do status do leito (ATIVO/VAGO/MANUT_*)
  @Column({ type: "enum", enum: StatusLeito, nullable: true })
  leitoStatus?: StatusLeito | null;

  // ===== campos da AVALIACAO =====
  @Column({ type: "varchar", length: 60, nullable: true })
  scp?: string | null;

  @Column({ type: "int", nullable: true })
  totalPontos?: number | null;

  @Column({ type: "enum", enum: ClassificacaoCuidado, nullable: true })
  classificacao?: ClassificacaoCuidado | null;

  @Column({ type: "jsonb", nullable: true })
  itens?: Record<string, number> | null;

  @Column({ type: "uuid", nullable: true })
  autorId?: string | null;

  @Column({ type: "varchar", length: 180, nullable: true })
  autorNome?: string | null;

  @Column({ type: "timestamptz" })
  inicio!: Date;

  @Column({ type: "timestamptz", nullable: true })
  fim?: Date;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
