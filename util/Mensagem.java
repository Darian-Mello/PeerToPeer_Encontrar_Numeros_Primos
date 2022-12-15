/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Mensagem implements Serializable {

    private String operacao;
    private Status status;

    /* 
     chave : Object
     */
    Map<String, String> params;

    public Mensagem(String operacao) {
        this.operacao = operacao;
        params = new HashMap<>();
    }

    public Mensagem() {
        params = new HashMap<>();
    }

    public String getOperacao() {
        return operacao;
    }

    public void setStatus(Status s) {
        this.status = s;
    }

    public Status getStatus() {
        return status;
    }
    /*
     "NOME" --> "José"
     "IDADE" --> 35
     */

    public void setParam(String chave, String valor) {
        params.put(chave, valor);
    }

    public String getParam(String chave) {
        return params.get(chave);
    }

    public static Mensagem parseString(String protocolo) {

        String p[] = protocolo.split(";");
        Mensagem m = new Mensagem(p[0]);
        try {
            for (int i = 1; i < p.length; i++) {
                String chaveValor[] = p[i].split(":");
                m.setParam(chaveValor[0], chaveValor[1]);
            }
        } catch (Exception e) {
            System.out.println("Falha no parser da mensagem: " + e.getMessage());
            return null;
        }
        return m;
    }
       public static Mensagem clientParseString(String protocolo) {

        String p[] = protocolo.split(";");
        Mensagem m = new Mensagem(p[0]);
        if( p[1].equals("OK") ){
            m.setStatus(Status.OK);
        }else
            m.setStatus(Status.ERROR);
        
        
        try {
            
                for (int i = 2; i < p.length; i++) {
                    String chaveValor[] = p[i].split(":");
                    m.setParam(chaveValor[0], chaveValor[1]);
                }
            
        } catch (Exception e) {
            System.out.println("Falha no parser da mensagem: " + e.getMessage());
            return null;
        }
        return m;
    }

    @Override
    public String toString() {
        String m = operacao;
        m += ";" + status;

        m += ";";
        for (String p : params.keySet()) {
            m += p + ": " + params.get(p) + ";";
        }
        return m;
    }

}
