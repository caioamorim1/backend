import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
  UpdateDateColumn,
  JoinColumn,
} from "typeorm";
import { SitioFuncional } from "./SitioFuncional";

export type CategoriaDistribuicao = "ENF" | "TEC";

@Entity("sitio_distribuicoes")
export class SitioDistribuicao {
  @PrimaryGeneratedColumn("uuid")
  id!: string;

  @Column({ name: "sitio_id" })
  sitioId!: string;

  @ManyToOne(() => SitioFuncional, (sitio) => sitio.distribuicoes, {
    nullable: false,
    onDelete: "CASCADE",
  })
  @JoinColumn({ name: "sitio_id" })
  sitio!: SitioFuncional;

  @Column({ type: "varchar", length: 3 })
  categoria!: CategoriaDistribuicao;

  @Column({ type: "int", default: 0 })
  segSexManha!: number;

  @Column({ type: "int", default: 0 })
  segSexTarde!: number;

  @Column({ type: "int", default: 0 })
  segSexNoite1!: number;

  @Column({ type: "int", default: 0 })
  segSexNoite2!: number;

  @Column({ type: "int", default: 0 })
  sabDomManha!: number;

  @Column({ type: "int", default: 0 })
  sabDomTarde!: number;

  @Column({ type: "int", default: 0 })
  sabDomNoite1!: number;

  @Column({ type: "int", default: 0 })
  sabDomNoite2!: number;

  @CreateDateColumn()
  created_at!: Date;

  @UpdateDateColumn()
  updated_at!: Date;
}
