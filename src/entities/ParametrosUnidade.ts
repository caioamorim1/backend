import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";
import { UnidadeInternacao } from "./UnidadeInternacao";

@Entity("parametros_unidade")
export class ParametrosUnidade {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @ManyToOne(() => UnidadeInternacao, { nullable: false, onDelete: "CASCADE" })
  unidade!: UnidadeInternacao;

  @Column({ type: "text", nullable: true })
  nome_enfermeiro?: string;

  @Column({ type: "text", nullable: true })
  numero_coren?: string;

  @Column({ type: "boolean", nullable: true })
  aplicarIST?: boolean;

  // IST stored as decimal 0..1 when applicable (e.g., 0.15 for 15%)
  @Column({ type: "decimal", precision: 5, scale: 2, nullable: true })
  ist?: number;

  // Dias da semana usados no cálculo (por padrão 7)
  @Column({ type: "int", nullable: true })
  diasSemana?: number;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
