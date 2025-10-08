import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";
import { UnidadeInternacao } from "./UnidadeInternacao";

export enum StatusLeito {
  ATIVO = "ATIVO",
  PENDENTE = "PENDENTE",
  VAGO = "VAGO",
  INATIVO = "INATIVO",
}

@Entity("leitos")
export class Leito {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @ManyToOne(() => UnidadeInternacao, (u) => u.leitos, {
    nullable: false,
    onDelete: "CASCADE",
  })
  unidade!: UnidadeInternacao;

  @Column({ type: "varchar", length: 20 })
  numero!: string;

  @Column({ type: "enum", enum: StatusLeito, default: StatusLeito.PENDENTE })
  status!: StatusLeito;

  @Column({ type: "text", nullable: true })
  justificativa?: string | null;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;

  // histórico de ocupações via sessões (AvaliacaoSCP) deixou de usar entidade Internacao
}
