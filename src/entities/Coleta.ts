import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
  UpdateDateColumn,
} from "typeorm";
import { Questionario } from "./Questionario";
import { UnidadeInternacao } from "./UnidadeInternacao";
import { SitioFuncional } from "./SitioFuncional";
import { Colaborador } from "./Colaborador";

@Entity("coletas")
export class Coleta {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  // Relacionamento com o questionário aplicado
  @ManyToOne(() => Questionario, { eager: true, nullable: false })
  questionario!: Questionario;

  // Pode ser feito em unidade ou sitio (um dos dois, nunca ambos)
  @ManyToOne(() => UnidadeInternacao, { nullable: true })
  unidade?: UnidadeInternacao;

  @ManyToOne(() => SitioFuncional, { nullable: true })
  sitio?: SitioFuncional;

  // Nome do local (redundante para facilitar busca/relatórios)
  @Column({ type: "varchar", length: 255 })
  localNome!: string;

  // Respostas: array de objetos { perguntaId, valor, comentario?, fotoUrl? }
  @Column({ type: "jsonb" })
  respostas!: Array<{
    perguntaId: string;
    valor: any;
    comentario?: string;
    fotoUrl?: string;
  }>;
  @ManyToOne(() => Colaborador, {
    nullable: true,
    onDelete: "SET NULL",
    eager: true,
  })
  colaborador?: Colaborador | null;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
